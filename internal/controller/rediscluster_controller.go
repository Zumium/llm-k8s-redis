package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
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
	finalizer = "redis.example.com/redis-cluster-finalizer"

	statusConflictRequeueAfter = time.Second

	ConditionReady   = "Ready"
	ConditionPlanned = "Planned"
	ConditionHealthy = "Healthy"
)

type RedisClusterReconciler struct {
	client.Client
	Scheme                  *runtime.Scheme
	Planner                 planner.Planner
	Driver                  Driver
	ValidatePlan            func(*plan.Plan, any) error
	Recorder                events.EventRecorder
	TopologyRefreshInterval time.Duration
	TopologyStaleThreshold  time.Duration
	PlanValidationRetries   int
}

func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", req.NamespacedName)

	if r.Planner == nil {
		r.Planner = planner.NoopPlanner{}
	}
	if r.Driver == nil {
		r.Driver = &ActionExecutor{Client: r.Client, Scheme: r.Scheme}
	}
	if r.ValidatePlan == nil {
		r.ValidatePlan = plan.NewValidator().Validate
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
		if err := r.Driver.ObserveTopology(ctx, &cluster); err != nil {
			logger.Error(err, "lazy topology refresh failed")
		}
	}

	spec := toClusterSpec(&cluster)

	active := cluster.Status.ActivePlan

	if active != nil && planState(active) == plan.PlanStateRunning {
		return r.executeNextStep(ctx, &cluster, active)
	}

	if active != nil {
		if active.TargetGeneration != cluster.Generation {
			cluster.Status.ActivePlan = nil
			setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "Replanning", "active plan targets an older generation")
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", "active plan targets an older generation")
			return r.finish(ctx, &cluster, ctrl.Result{Requeue: true}, nil)
		}
		switch planState(active) {
		case plan.PlanStateCompleted:
			if topologyMatchesSpec(cluster.Status.Topology, spec) {
				active.Status = string(plan.PlanStateCompleted)
				cluster.Status.ObservedGeneration = cluster.Generation
				setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "NoPlanNeeded", "topology already matches spec")
				setCondition(&cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "cluster matches desired topology")
				return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
			}
			cluster.Status.ActivePlan = nil
			setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "Replanning", "topology drifted")
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", "existing plan no longer valid")
			return r.finish(ctx, &cluster, ctrl.Result{Requeue: true}, nil)
		case plan.PlanStateFailed:
			if topologyMatchesSpec(cluster.Status.Topology, spec) {
				cluster.Status.ActivePlan = nil
				cluster.Status.ObservedGeneration = cluster.Generation
				setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "NoPlanNeeded", "topology already matches spec")
				setCondition(&cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "cluster matches desired topology")
				return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
			}
			cluster.Status.ActivePlan = nil
			setCondition(&cluster, ConditionReady, metav1.ConditionFalse, "Replanning", "topology drifted")
			setCondition(&cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", "existing plan no longer valid")
			return r.finish(ctx, &cluster, ctrl.Result{Requeue: true}, nil)
		}
	}

	if cluster.Status.ActivePlan == nil {
		return r.reconcilePlan(ctx, &cluster, spec)
	}

	return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
}

func (r *RedisClusterReconciler) reconcilePlan(ctx context.Context, cluster *v1alpha1.RedisCluster, spec plan.ClusterSpec) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)

	req := toPlannerRequest(cluster, spec)
	nodes, err := r.Driver.CollectObservedNodes(ctx, cluster)
	if err != nil {
		logger.Error(err, "collect observed nodes failed")
		setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "ObservedNodesFailed", err.Error())
		cluster.Status.ObservedGeneration = cluster.Generation
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
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
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerFailed", err.Error())
			cluster.Status.ObservedGeneration = cluster.Generation
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		if newPlan == nil {
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerEmpty", "planner returned no plan")
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		if err := r.ValidatePlan(newPlan, validationContext(cluster, spec, nodes)); err != nil {
			if topologyMatchesSpec(cluster.Status.Topology, spec) {
				logger.Info("plan rejected but topology already matches spec, marking ready")
				cluster.Status.ObservedGeneration = cluster.Generation
				setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "NoPlanNeeded", "topology already matches spec")
				setCondition(cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "cluster matches desired topology")
				return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
			}
			if attempt < validationRetries {
				req.ValidationFeedback = append(req.ValidationFeedback, planner.ValidationFeedback{RejectedPlan: newPlan, Error: err.Error()})
				continue
			}
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanRejected", err.Error())
			cluster.Status.ObservedGeneration = cluster.Generation
			r.event(cluster, "PlanRejected", err.Error())
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		advanceNextPodOrdinalFromPlan(cluster, newPlan)
		cluster.Status.ActivePlan, err = planToStatus(newPlan)
		if err != nil {
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanPersistFailed", err.Error())
			cluster.Status.ObservedGeneration = cluster.Generation
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		cluster.Status.ObservedGeneration = cluster.Generation
		setCondition(cluster, ConditionPlanned, metav1.ConditionTrue, "PlanAccepted", "plan passed validation")
		return r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
	}

	return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
}

func (r *RedisClusterReconciler) executeNextStep(ctx context.Context, cluster *v1alpha1.RedisCluster, active *v1alpha1.PlanStatus) (ctrl.Result, error) {
	idx := nextPendingStep(active)
	if idx < 0 {
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 5 * time.Second}, nil)
	}

	step := active.Steps[idx]
	planModel, err := statusToPlan(active)
	if err != nil {
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		active.CurrentStep = step.ID
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "PlanRestoreFailed", err.Error())
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}
	outcome, err := r.Driver.ExecuteStep(ctx, cluster, planModel, idx)
	if err != nil {
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		active.CurrentStep = step.ID
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", err.Error())
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}
	setStep(active, idx, string(outcome.Status), outcome.Message)
	active.CurrentStep = step.ID

	switch outcome.Status {
	case plan.StepStateCompleted:
		return r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
	case plan.StepStateFailed:
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", outcome.Message)
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	case plan.StepStateRunning:
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 5 * time.Second}, nil)
	default:
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 5 * time.Second}, nil)
	}
}

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

func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.RedisCluster{}).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
