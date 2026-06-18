// Package controller implements the RedisCluster reconciler.
//
// The reconciler is intentionally agnostic of how plans are produced and how
// steps are executed: it wires a Planner (LLM-backed), a deterministic
// Validator, and an Executor (Redis action executors) through interfaces. This
// file contains the reconciliation flow and the K8S-level housekeeping
// (finalizer, namespace ownership, status persistence) that does not depend on
// Redis specifics.
package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/planner"
)

const (
	// finalizer gates cluster deletion until the owned namespace is gone.
	finalizer = "redis.example.com/redis-cluster-finalizer"

	ConditionReady   = "Ready"
	ConditionPlanned = "Planned"
	ConditionHealthy = "Healthy"
)

// RedisClusterReconciler reconciles a RedisCluster object by requesting a plan
// from the Planner, validating it, persisting it into status.activePlan, and
// driving the Executor one pending step per reconcile.
type RedisClusterReconciler struct {
	client.Client
	Scheme    *runtime.Scheme
	Planner   planner.Planner
	Executor  Executor
	Validator Validator
	Recorder  events.EventRecorder
}

// Reconcile advances a RedisCluster by at most one plan step.
//
// Flow:
//  1. Fetch the RedisCluster (cluster-scoped).
//  2. If deleting, drain the owned namespace then remove the finalizer.
//  3. Ensure the finalizer and the dedicated namespace (name == cluster name).
//  4. If no active plan, ask the Planner, validate, persist activePlan.
//  5. Otherwise execute the next pending step and record its outcome.
//
// Each reconcile performs at most one mutating K8S write plus one status write.
func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", req.NamespacedName)

	if r.Planner == nil {
		r.Planner = planner.NoopPlanner{}
	}
	if r.Executor == nil {
		r.Executor = NoopExecutor{}
	}
	if r.Validator == nil {
		r.Validator = plan.NewValidator()
	}

	var cluster v1alpha1.RedisCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			return ctrl.Result{}, nil
		}
		return ctrl.Result{}, err
	}

	if !cluster.DeletionTimestamp.IsZero() {
		return r.reconcileDelete(ctx, &cluster)
	}

	if !controllerutil.ContainsFinalizer(&cluster, finalizer) {
		controllerutil.AddFinalizer(&cluster, finalizer)
		if err := r.Update(ctx, &cluster); err != nil {
			return ctrl.Result{}, err
		}
		return ctrl.Result{Requeue: true}, nil
	}

	if err := r.ensureNamespace(ctx, &cluster); err != nil {
		return ctrl.Result{}, fmt.Errorf("ensure namespace: %w", err)
	}

	spec := toClusterSpec(&cluster)

	if cluster.Status.Phase == "" {
		cluster.Status.Phase = v1alpha1.PhaseCreating
	}

	// No active plan: produce, validate and persist one.
	if cluster.Status.ActivePlan == nil {
		newPlan, err := r.Planner.Plan(ctx, toPlannerRequest(&cluster, spec))
		if err != nil {
			logger.Error(err, "planner returned an error")
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerFailed", err.Error())
			cluster.Status.Phase = v1alpha1.PhaseFailed
			cluster.Status.ObservedGeneration = cluster.Generation
			return r.finish(ctx, &cluster, ctrl.Result{}, nil)
		}
		if newPlan == nil {
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerEmpty", "planner returned no plan")
			return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		if err := r.Validator.Validate(newPlan, spec); err != nil {
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlanRejected", err.Error())
			cluster.Status.Phase = v1alpha1.PhaseFailed
			cluster.Status.ObservedGeneration = cluster.Generation
			r.event(&cluster, "PlanRejected", err.Error())
			return r.finish(ctx, &cluster, ctrl.Result{}, nil)
		}
		cluster.Status.ActivePlan, err = planToStatus(newPlan)
		if err != nil {
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlanPersistFailed", err.Error())
			cluster.Status.Phase = v1alpha1.PhaseFailed
			cluster.Status.ObservedGeneration = cluster.Generation
			return r.finish(ctx, &cluster, ctrl.Result{}, nil)
		}
		cluster.Status.Phase = derivePhase(newPlan.Operation)
		cluster.Status.ObservedGeneration = cluster.Generation
		setCondition(&cluster, ConditionPlanned, metav1.ConditionTrue, "PlanAccepted", "plan passed validation")
		return r.finish(ctx, &cluster, ctrl.Result{Requeue: true}, nil)
	}

	// Active plan present: evaluate aggregate state, then drive one step.
	active := cluster.Status.ActivePlan
	switch planState(active) {
	case plan.PlanStateCompleted:
		active.Status = string(plan.PlanStateCompleted)
		cluster.Status.Phase = v1alpha1.PhaseReady
		cluster.Status.ObservedGeneration = cluster.Generation
		setCondition(&cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "plan completed")
		return r.finish(ctx, &cluster, ctrl.Result{}, nil)
	case plan.PlanStateFailed:
		active.Status = string(plan.PlanStateFailed)
		cluster.Status.Phase = v1alpha1.PhaseFailed
		setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "PlanFailed", "plan has failed steps")
		return r.finish(ctx, &cluster, ctrl.Result{}, nil)
	}

	idx := nextPendingStep(active)
	if idx < 0 {
		return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: 5 * time.Second}, nil)
	}

	step := active.Steps[idx]
	planModel, err := statusToPlan(active)
	if err != nil {
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		active.CurrentStep = step.ID
		cluster.Status.Phase = v1alpha1.PhaseFailed
		setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "PlanRestoreFailed", err.Error())
		return r.finish(ctx, &cluster, ctrl.Result{}, nil)
	}
	outcome, err := r.Executor.ExecuteStep(ctx, &cluster, planModel, idx)
	if err != nil {
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		active.CurrentStep = step.ID
		cluster.Status.Phase = v1alpha1.PhaseFailed
		setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", err.Error())
		return r.finish(ctx, &cluster, ctrl.Result{}, nil)
	}
	setStep(active, idx, string(outcome.Status), outcome.Message)
	active.CurrentStep = step.ID

	switch outcome.Status {
	case plan.StepStateCompleted:
		return r.finish(ctx, &cluster, ctrl.Result{Requeue: true}, nil)
	case plan.StepStateFailed:
		cluster.Status.Phase = v1alpha1.PhaseFailed
		setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", outcome.Message)
		return r.finish(ctx, &cluster, ctrl.Result{}, nil)
	case plan.StepStateRunning:
		return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: 5 * time.Second}, nil)
	default:
		return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: 5 * time.Second}, nil)
	}
}

// ensureNamespace makes sure a namespace named after the cluster exists and is
// owned by the RedisCluster CR, so that K8S garbage-collects it on deletion.
func (r *RedisClusterReconciler) ensureNamespace(ctx context.Context, cluster *v1alpha1.RedisCluster) error {
	var ns corev1.Namespace
	err := r.Get(ctx, client.ObjectKey{Name: cluster.Name}, &ns)
	if err == nil {
		return nil
	}
	if !apierrors.IsNotFound(err) {
		return err
	}
	nsObj := &corev1.Namespace{
		ObjectMeta: metav1.ObjectMeta{
			Name: cluster.Name,
			Labels: map[string]string{
				"app.kubernetes.io/managed-by": "redis-cluster-controller",
				"redis.example.com/cluster":    cluster.Name,
			},
		},
	}
	if err := ctrl.SetControllerReference(cluster, nsObj, r.Scheme); err != nil {
		return fmt.Errorf("set namespace owner reference: %w", err)
	}
	return r.Create(ctx, nsObj)
}

// reconcileDelete waits for the dedicated namespace to be torn down, then
// removes the finalizer so the RedisCluster CR itself can be collected.
func (r *RedisClusterReconciler) reconcileDelete(ctx context.Context, cluster *v1alpha1.RedisCluster) (ctrl.Result, error) {
	if !controllerutil.ContainsFinalizer(cluster, finalizer) {
		return ctrl.Result{}, nil
	}
	var ns corev1.Namespace
	err := r.Get(ctx, client.ObjectKey{Name: cluster.Name}, &ns)
	if err == nil {
		if ns.DeletionTimestamp.IsZero() {
			if derr := r.Delete(ctx, &ns); derr != nil && !apierrors.IsNotFound(derr) {
				return ctrl.Result{}, derr
			}
		}
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if !apierrors.IsNotFound(err) {
		return ctrl.Result{}, err
	}
	controllerutil.RemoveFinalizer(cluster, finalizer)
	if err := r.Update(ctx, cluster); err != nil {
		return ctrl.Result{}, err
	}
	return ctrl.Result{}, nil
}

// finish persists status when dirty and returns the reconcile result.
func (r *RedisClusterReconciler) finish(ctx context.Context, cluster *v1alpha1.RedisCluster, res ctrl.Result, err error) (ctrl.Result, error) {
	if uerr := r.Status().Update(ctx, cluster); uerr != nil && err == nil {
		return res, uerr
	}
	return res, err
}

func (r *RedisClusterReconciler) event(cluster *v1alpha1.RedisCluster, reason, msg string) {
	if r.Recorder != nil {
		r.Recorder.Eventf(cluster, nil, "Warning", reason, "Reconcile", msg)
	}
}

// SetupWithManager registers the reconciler, watching the RedisCluster and the
// namespace/pod resources it owns.
func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.RedisCluster{}).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}

func toClusterSpec(c *v1alpha1.RedisCluster) plan.ClusterSpec {
	return plan.ClusterSpec{
		Name:             c.Name,
		Generation:       c.Generation,
		Shards:           c.Spec.Shards,
		ReplicasPerShard: c.Spec.ReplicasPerShard,
		Image:            c.Spec.Image,
		MemorySize:       c.Spec.MemorySize,
	}
}

// toPlannerRequest builds a planner.Request from the current cluster state.
// The Operation is derived from the cluster's observed state: if there is no
// topology yet, the operation is Create; otherwise it would be a topology/
// image/memory change. For now we default to Create since incremental
// operations are not yet implemented.
func toPlannerRequest(c *v1alpha1.RedisCluster, spec plan.ClusterSpec) planner.Request {
	op := plan.OpCreate
	if c.Status.Topology != nil && len(c.Status.Topology.Shards) > 0 {
		op = plan.OpUpdateMemorySize // placeholder; refined when scale/upgrade executors land
	}
	return planner.Request{
		Cluster:   c,
		Spec:      spec,
		Operation: op,
		ObservedState: planner.ObservedState{
			Topology:   c.Status.Topology,
			ActivePlan: c.Status.ActivePlan,
		},
	}
}

func derivePhase(op plan.Operation) v1alpha1.RedisClusterPhase {
	switch op {
	case plan.OpCreate:
		return v1alpha1.PhaseCreating
	case plan.OpDelete:
		return v1alpha1.PhaseDeleting
	case plan.OpScaleOut, plan.OpScaleIn:
		return v1alpha1.PhaseScaling
	case plan.OpUpdateMemorySize:
		return v1alpha1.PhaseUpgrading
	default:
		return v1alpha1.PhaseCreating
	}
}

func setCondition(c *v1alpha1.RedisCluster, t string, s metav1.ConditionStatus, reason, msg string) {
	meta.SetStatusCondition(&c.Status.Conditions, metav1.Condition{
		Type:               t,
		Status:             s,
		Reason:             reason,
		Message:            msg,
		ObservedGeneration: c.Generation,
	})
}

func setStep(ps *v1alpha1.PlanStatus, idx int, status, msg string) {
	if idx < 0 || idx >= len(ps.Steps) {
		return
	}
	s := &ps.Steps[idx]
	if s.Status != status {
		s.LastTransitionTime = metav1.Now()
	}
	s.Status = status
	s.Message = msg
}

// nextPendingStep returns the index of the first non-completed step, or -1 if
// the plan is complete or halted on a failed step.
func nextPendingStep(ps *v1alpha1.PlanStatus) int {
	for i, s := range ps.Steps {
		if s.Status == string(plan.StepStateFailed) {
			return -1
		}
		if s.Status != string(plan.StepStateCompleted) {
			return i
		}
	}
	return -1
}

func planState(ps *v1alpha1.PlanStatus) plan.PlanState {
	hasFailed, allDone := false, true
	for _, s := range ps.Steps {
		switch s.Status {
		case string(plan.StepStateFailed):
			hasFailed, allDone = true, false
		case string(plan.StepStateCompleted):
			// completed
		default:
			allDone = false
		}
	}
	if hasFailed {
		return plan.PlanStateFailed
	}
	if allDone && len(ps.Steps) > 0 {
		return plan.PlanStateCompleted
	}
	return plan.PlanStateRunning
}

func planToStatus(p *plan.Plan) (*v1alpha1.PlanStatus, error) {
	steps := make([]v1alpha1.StepStatus, len(p.Steps))
	for i, s := range p.Steps {
		raw, err := json.Marshal(s.Params)
		if err != nil {
			return nil, fmt.Errorf("marshal step %q params: %w", s.ID, err)
		}
		steps[i] = v1alpha1.StepStatus{
			ID:     s.ID,
			Action: string(s.Action),
			Status: string(plan.StepStatePending),
			Params: apiextensionsv1.JSON{Raw: raw},
		}
	}
	return &v1alpha1.PlanStatus{
		ID:               p.PlanID,
		Operation:        string(p.Operation),
		Status:           string(plan.PlanStateRunning),
		TargetGeneration: p.TargetGeneration,
		Summary:          p.Summary,
		CurrentStep:      firstPendingID(steps),
		Steps:            steps,
	}, nil
}

func statusToPlan(ps *v1alpha1.PlanStatus) (*plan.Plan, error) {
	steps := make([]plan.Step, len(ps.Steps))
	for i, s := range ps.Steps {
		params := map[string]any{}
		if len(s.Params.Raw) > 0 {
			if err := json.Unmarshal(s.Params.Raw, &params); err != nil {
				return nil, fmt.Errorf("unmarshal step %q params: %w", s.ID, err)
			}
		}
		steps[i] = plan.Step{
			ID:     s.ID,
			Action: plan.ActionType(s.Action),
			Params: params,
		}
	}
	return &plan.Plan{
		DSLVersion:       plan.DSLVersion,
		PlanID:           ps.ID,
		Operation:        plan.Operation(ps.Operation),
		TargetGeneration: ps.TargetGeneration,
		Summary:          ps.Summary,
		Steps:            steps,
	}, nil
}

func firstPendingID(steps []v1alpha1.StepStatus) string {
	for _, s := range steps {
		if s.Status != string(plan.StepStateCompleted) {
			return s.ID
		}
	}
	return ""
}
