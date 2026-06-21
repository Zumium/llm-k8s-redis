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
	"maps"
	"strconv"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
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

	statusConflictRequeueAfter = time.Second

	ConditionReady   = "Ready"
	ConditionPlanned = "Planned"
	ConditionHealthy = "Healthy"
)

// RedisClusterReconciler reconciles a RedisCluster object by requesting a plan
// from the Planner, validating it, persisting it into status.activePlan, and
// driving the Executor one pending step per reconcile.
type RedisClusterReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Planner                 planner.Planner
	Executor                Executor
	Observer                Observer
	Validator               Validator
	Recorder                events.EventRecorder
	TopologyRefreshInterval time.Duration
	TopologyStaleThreshold  time.Duration
	PlanValidationRetries   int
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
	if r.Observer == nil {
		r.Observer = noopObserver{}
	}
	if r.Validator == nil {
		r.Validator = plan.NewValidator()
	}
	if r.TopologyRefreshInterval <= 0 {
		r.TopologyRefreshInterval = 60 * time.Second
	}
	if r.TopologyStaleThreshold <= 0 {
		r.TopologyStaleThreshold = 10 * time.Second
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

	bumpNextPodOrdinal(&cluster)

	if r.shouldRefreshTopology(ctx, &cluster) {
		if err := r.Observer.ObserveTopology(ctx, &cluster); err != nil {
			logger.Error(err, "lazy topology refresh failed")
		}
	}

	spec := toClusterSpec(&cluster)

	// No active plan: produce, validate and persist one.
	if cluster.Status.ActivePlan == nil {
		req := toPlannerRequest(&cluster, spec)
		nodes, err := r.Observer.CollectObservedNodes(ctx, &cluster)
		if err != nil {
			logger.Error(err, "collect observed nodes failed")
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "ObservedNodesFailed", err.Error())
			cluster.Status.ObservedGeneration = cluster.Generation
			return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		req.ObservedState.Nodes = nodes
		validationRetries := r.PlanValidationRetries
		if validationRetries < 0 {
			validationRetries = 0
		}
		for attempt := 0; attempt <= validationRetries; attempt++ {
			newPlan, err := r.Planner.Plan(ctx, req)
			if err != nil {
				logger.Error(err, "planner returned an error")
				setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerFailed", err.Error())
				cluster.Status.ObservedGeneration = cluster.Generation
				return r.finish(ctx, &cluster, ctrl.Result{}, nil)
			}
			if newPlan == nil {
				setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerEmpty", "planner returned no plan")
				return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
			}
			if err := r.Validator.Validate(newPlan, validationContext(&cluster, spec, nodes)); err != nil {
				if topologyMatchesSpec(cluster.Status.Topology, spec) {
					logger.Info("plan rejected but topology already matches spec, marking ready")
					cluster.Status.ObservedGeneration = cluster.Generation
					setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "NoPlanNeeded", "topology already matches spec")
					setCondition(&cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "cluster matches desired topology")
					return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
				}
				if attempt < validationRetries {
					req.ValidationFeedback = append(req.ValidationFeedback, planner.ValidationFeedback{RejectedPlan: newPlan, Error: err.Error()})
					continue
				}
				setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlanRejected", err.Error())
				cluster.Status.ObservedGeneration = cluster.Generation
				r.event(&cluster, "PlanRejected", err.Error())
				return r.finish(ctx, &cluster, ctrl.Result{}, nil)
			}
			advanceNextPodOrdinalFromPlan(&cluster, newPlan)
			cluster.Status.ActivePlan, err = planToStatus(newPlan)
			if err != nil {
				setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlanPersistFailed", err.Error())
				cluster.Status.ObservedGeneration = cluster.Generation
				return r.finish(ctx, &cluster, ctrl.Result{}, nil)
			}
			cluster.Status.ObservedGeneration = cluster.Generation
			setCondition(&cluster, ConditionPlanned, metav1.ConditionTrue, "PlanAccepted", "plan passed validation")
			return r.finish(ctx, &cluster, ctrl.Result{Requeue: true}, nil)
		}
	}

	// Active plan present: evaluate aggregate state, then drive one step.
	active := cluster.Status.ActivePlan
	if active.TargetGeneration != cluster.Generation {
		cluster.Status.ActivePlan = nil
		setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "Replanning", "active plan targets an older generation")
		setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", "active plan targets an older generation")
		return r.finish(ctx, &cluster, ctrl.Result{Requeue: true}, nil)
	}
	switch planState(active) {
	case plan.PlanStateCompleted:
		active.Status = string(plan.PlanStateCompleted)
		cluster.Status.ObservedGeneration = cluster.Generation
		setCondition(&cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "plan completed")
		return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
	case plan.PlanStateFailed:
		if topologyMatchesSpec(cluster.Status.Topology, spec) {
			cluster.Status.ActivePlan = nil
			cluster.Status.ObservedGeneration = cluster.Generation
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "NoPlanNeeded", "current topology already matches spec")
			setCondition(&cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "cluster matches desired topology")
			return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
		}
		active.Status = string(plan.PlanStateFailed)
		setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "PlanFailed", "plan has failed steps")
		return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
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
		setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "PlanRestoreFailed", err.Error())
		return r.finish(ctx, &cluster, ctrl.Result{}, nil)
	}
	outcome, err := r.Executor.ExecuteStep(ctx, &cluster, planModel, idx)
	if err != nil {
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		active.CurrentStep = step.ID
		setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", err.Error())
		return r.finish(ctx, &cluster, ctrl.Result{}, nil)
	}
	setStep(active, idx, string(outcome.Status), outcome.Message)
	active.CurrentStep = step.ID

	switch outcome.Status {
	case plan.StepStateCompleted:
		return r.finish(ctx, &cluster, ctrl.Result{Requeue: true}, nil)
	case plan.StepStateFailed:
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
		if apierrors.IsConflict(uerr) {
			return ctrl.Result{RequeueAfter: statusConflictRequeueAfter}, nil
		}
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

func validationContext(c *v1alpha1.RedisCluster, spec plan.ClusterSpec, nodes []plan.ObservedNode) plan.ValidationContext {
	return plan.ValidationContext{
		Spec:           spec,
		Topology:       toPlanTopology(c.Status.Topology),
		NextPodOrdinal: int(c.Status.NextPodOrdinal),
		ObservedNodes:  nodes,
	}
}

func toPlanTopology(t *v1alpha1.ClusterTopology) *plan.ClusterTopology {
	if t == nil {
		return nil
	}
	out := &plan.ClusterTopology{Shards: make([]plan.ShardTopology, 0, len(t.Shards))}
	for _, sh := range t.Shards {
		ps := plan.ShardTopology{
			ID:     sh.ID,
			Master: toPlanNode(sh.Master),
		}
		for _, r := range sh.Replicas {
			ps.Replicas = append(ps.Replicas, toPlanNode(r))
		}
		out.Shards = append(out.Shards, ps)
	}
	return out
}

func toPlanNode(n v1alpha1.NodeTopology) plan.NodeTopology {
	return plan.NodeTopology{
		Pod:    n.Pod,
		NodeID: n.NodeID,
		Slots:  n.Slots,
		Ready:  n.Ready,
	}
}

func toPlannerRequest(c *v1alpha1.RedisCluster, spec plan.ClusterSpec) planner.Request {
	return planner.Request{
		Spec: spec,
		ObservedState: planner.ObservedState{
			ActivePlan:     c.Status.ActivePlan,
			NextPodOrdinal: int(c.Status.NextPodOrdinal),
		},
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

// shouldRefreshTopology decides whether a lazy observe-only refresh of
// status.topology is appropriate right now. It skips refresh when a plan is
// actively executing, when the cluster has not yet bootstrapped a topology,
// or when the last observation is still fresh enough.
//
// Pod-set drift bypasses the stale threshold: if the managed Pods (names or
// Ready states) no longer match what status.topology recorded, the next
// reconcile forces an immediate ObserveTopology so Pod deletion/status flips
// are reflected without waiting for the stale gate or the slow requeue.
func (r *RedisClusterReconciler) shouldRefreshTopology(ctx context.Context, c *v1alpha1.RedisCluster) bool {
	if c.Status.Topology == nil {
		return false
	}
	active := c.Status.ActivePlan
	if active != nil && planState(active) == plan.PlanStateRunning {
		return false
	}
	if r.podSetDrifted(ctx, c) {
		return true
	}
	elapsed := time.Since(c.Status.TopologyObservedAt.Time)
	return elapsed >= r.TopologyStaleThreshold
}

// podSetDrifted reports whether the live managed Pod set (names + Ready
// states) differs from what status.topology last recorded. A List error is
// treated as "no drift" so a transient cache failure does not force a Redis
// observe on every reconcile. A nil topology is treated as "no drift" so the
// caller can delegate the nil-guard to shouldRefreshTopology without
// duplicating it here.
func (r *RedisClusterReconciler) podSetDrifted(ctx context.Context, c *v1alpha1.RedisCluster) bool {
	if c.Status.Topology == nil {
		return false
	}
	selector, err := labels.Parse(labelCluster + "=" + c.Name)
	if err != nil {
		return false
	}
	var podList corev1.PodList
	if err := r.List(ctx, &podList, &client.ListOptions{
		Namespace:     c.Name,
		LabelSelector: selector,
	}); err != nil {
		return false
	}
	live := livePodSignature(podList.Items)
	want := topologyPodSignature(c.Status.Topology)
	return !maps.Equal(live, want)
}

// livePodSignature builds a {podName -> ready} signature from the live Pods.
func livePodSignature(pods []corev1.Pod) map[string]bool {
	out := map[string]bool{}
	for i := range pods {
		out[pods[i].Name] = podReady(&pods[i])
	}
	return out
}

// topologyPodSignature builds a {podName -> ready} signature from the last
// observed topology. Entries with an empty Pod name (e.g. a Redis node the
// controller has not yet mapped to a K8S Pod) are skipped so they do not
// mask a real drift.
func topologyPodSignature(topo *v1alpha1.ClusterTopology) map[string]bool {
	out := map[string]bool{}
	if topo == nil {
		return out
	}
	for i := range topo.Shards {
		sh := &topo.Shards[i]
		if sh.Master.Pod != "" {
			out[sh.Master.Pod] = sh.Master.Ready
		}
		for j := range sh.Replicas {
			r := &sh.Replicas[j]
			if r.Pod != "" {
				out[r.Pod] = r.Ready
			}
		}
	}
	return out
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

func topologyMatchesSpec(topology *v1alpha1.ClusterTopology, spec plan.ClusterSpec) bool {
	if topology == nil || len(topology.Shards) == 0 {
		return false
	}
	if len(topology.Shards) != int(spec.Shards) {
		return false
	}
	for _, sh := range topology.Shards {
		if len(sh.Replicas) != int(spec.ReplicasPerShard) {
			return false
		}
		if sh.Master.Slots == "" {
			return false
		}
		if !sh.Master.Ready {
			return false
		}
		for _, r := range sh.Replicas {
			if !r.Ready {
				return false
			}
		}
	}
	return true
}

func bumpNextPodOrdinal(c *v1alpha1.RedisCluster) bool {
	before := c.Status.NextPodOrdinal
	max := int(before) - 1
	for _, pod := range statusTopologyPods(c.Status.Topology) {
		if n, ok := controllerRedisPodOrdinal(pod); ok && n > max {
			max = n
		}
	}
	if c.Status.ActivePlan != nil {
		p, err := statusToPlan(c.Status.ActivePlan)
		if err == nil {
			for _, s := range p.Steps {
				if s.Action != plan.ActionEnsureNode {
					continue
				}
				pod, ok := paramString(s.Params, "pod")
				if !ok {
					continue
				}
				if n, ok := controllerRedisPodOrdinal(pod); ok && n > max {
					max = n
				}
			}
		}
	}
	next := int32(max + 1)
	if next < 0 {
		next = 0
	}
	if next > c.Status.NextPodOrdinal {
		c.Status.NextPodOrdinal = next
		return true
	}
	return false
}

func advanceNextPodOrdinalFromPlan(c *v1alpha1.RedisCluster, p *plan.Plan) {
	if p == nil {
		return
	}
	next := c.Status.NextPodOrdinal
	for _, s := range p.Steps {
		if s.Action != plan.ActionEnsureNode {
			continue
		}
		pod, ok := paramString(s.Params, "pod")
		if !ok {
			continue
		}
		if n, ok := controllerRedisPodOrdinal(pod); ok && int32(n+1) > next {
			next = int32(n + 1)
		}
	}
	c.Status.NextPodOrdinal = next
}

func statusTopologyPods(t *v1alpha1.ClusterTopology) []string {
	if t == nil {
		return nil
	}
	out := []string{}
	for _, sh := range t.Shards {
		if sh.Master.Pod != "" {
			out = append(out, sh.Master.Pod)
		}
		for _, r := range sh.Replicas {
			if r.Pod != "" {
				out = append(out, r.Pod)
			}
		}
	}
	return out
}

func controllerRedisPodOrdinal(pod string) (int, bool) {
	if !strings.HasPrefix(pod, "redis-") {
		return 0, false
	}
	n, err := strconv.Atoi(strings.TrimPrefix(pod, "redis-"))
	if err != nil || n < 0 {
		return 0, false
	}
	return n, true
}
