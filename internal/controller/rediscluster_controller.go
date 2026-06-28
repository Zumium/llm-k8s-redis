package controller

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
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
	APIReader               client.Reader
	Scheme                  *runtime.Scheme
	Planner                 planner.Planner
	Driver                  Driver
	ValidatePlan            func(*plan.Plan, any) error
	Recorder                events.EventRecorder
	TopologyRefreshInterval time.Duration
	TopologyStaleThreshold  time.Duration
	PlanValidationRetries   int
}

func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues("rediscluster", req.NamespacedName)
	ctx = log.IntoContext(ctx, logger)
	defer func() {
		logger.Info("reconcile finished", "duration", time.Since(start), "requeue", res.Requeue, "requeueAfter", res.RequeueAfter, "error", err)
	}()
	logger.Info("reconcile started")

	if r.Planner == nil {
		r.Planner = planner.NoopPlanner{}
		logger.Info("default planner initialized")
	}
	if r.APIReader == nil {
		r.APIReader = r.Client
		logger.Info("default api reader initialized")
	}
	if r.Driver == nil {
		r.Driver = &ActionExecutor{Client: r.Client, Scheme: r.Scheme}
		logger.Info("default driver initialized")
	}
	if r.ValidatePlan == nil {
		r.ValidatePlan = plan.NewValidator().Validate
		logger.Info("default plan validator initialized")
	}
	if r.TopologyRefreshInterval <= 0 {
		r.TopologyRefreshInterval = 60 * time.Second
		logger.Info("default topology refresh interval initialized", "interval", r.TopologyRefreshInterval)
	}
	if r.TopologyStaleThreshold <= 0 {
		r.TopologyStaleThreshold = 10 * time.Second
		logger.Info("default topology stale threshold initialized", "threshold", r.TopologyStaleThreshold)
	}

	var cluster v1alpha1.RedisCluster
	if err := r.Get(ctx, req.NamespacedName, &cluster); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("rediscluster not found")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "get rediscluster failed")
		return ctrl.Result{}, err
	}
	logger.Info("rediscluster loaded", "generation", cluster.Generation, "observedGeneration", cluster.Status.ObservedGeneration)

	if !cluster.DeletionTimestamp.IsZero() {
		logger.Info("rediscluster deleting")
		return r.reconcileDelete(ctx, &cluster)
	}

	if !controllerutil.ContainsFinalizer(&cluster, finalizer) {
		logger.Info("adding finalizer", "finalizer", finalizer)
		controllerutil.AddFinalizer(&cluster, finalizer)
		if err := r.Update(ctx, &cluster); err != nil {
			logger.Error(err, "add finalizer failed")
			return ctrl.Result{}, err
		}
		logger.Info("finalizer added", "finalizer", finalizer)
		return ctrl.Result{Requeue: true}, nil
	}

	nsStart := time.Now()
	if err := r.ensureNamespace(ctx, &cluster); err != nil {
		logger.Error(err, "ensure namespace failed", "duration", time.Since(nsStart))
		return ctrl.Result{}, fmt.Errorf("ensure namespace: %w", err)
	}
	logger.Info("namespace ensured", "namespace", cluster.Name, "duration", time.Since(nsStart))

	if bumpNextPodOrdinal(&cluster) {
		logger.Info("next pod ordinal advanced", "nextPodOrdinal", cluster.Status.NextPodOrdinal)
	}

	if r.shouldRefreshTopology(ctx, &cluster) {
		refreshStart := time.Now()
		logger.Info("lazy topology refresh started")
		if err := r.Driver.ObserveTopology(ctx, &cluster); err != nil {
			logger.Error(err, "lazy topology refresh failed", "duration", time.Since(refreshStart))
		} else {
			logger.Info("lazy topology refresh finished", "duration", time.Since(refreshStart))
		}
	} else {
		logger.Info("lazy topology refresh skipped")
	}

	spec := toClusterSpec(&cluster)
	logger.Info("desired spec converted", "shards", spec.Shards, "replicasPerShard", spec.ReplicasPerShard, "image", spec.Image, "memorySize", spec.MemorySize)

	if res, err, handled := r.handleActivePlan(ctx, &cluster, spec); handled {
		return res, err
	}

	if cluster.Status.ActivePlan == nil {
		logger.Info("no active plan, reconciling plan")
		return r.reconcilePlanIfStillNeeded(ctx, &cluster)
	}

	logger.Info("no work this reconcile")
	return r.finish(ctx, &cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
}

func (r *RedisClusterReconciler) handleActivePlan(ctx context.Context, cluster *v1alpha1.RedisCluster, spec plan.ClusterSpec) (ctrl.Result, error, bool) {
	logger := log.FromContext(ctx)
	active := cluster.Status.ActivePlan
	if active == nil {
		return ctrl.Result{}, nil, false
	}
	if planState(active) == plan.PlanStateRunning {
		logger.Info("running active plan found", "planID", active.ID, "steps", len(active.Steps), "currentStep", active.CurrentStep)
		res, err := r.executeNextStep(ctx, cluster, active)
		return res, err, true
	}
	state := planState(active)
	logger.Info("active plan found", "planID", active.ID, "planState", state, "targetGeneration", active.TargetGeneration, "steps", len(active.Steps))
	if active.TargetGeneration != cluster.Generation {
		cluster.Status.ActivePlan = nil
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "Replanning", "active plan targets an older generation")
		setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", "active plan targets an older generation")
		logger.Info("active plan superseded by generation", "planID", active.ID, "targetGeneration", active.TargetGeneration, "generation", cluster.Generation)
		res, err := r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
		return res, err, true
	}
	switch state {
	case plan.PlanStateCompleted:
		if topologyMatchesSpec(cluster.Status.Topology, spec) {
			active.Status = string(plan.PlanStateCompleted)
			markNoPlanNeeded(cluster, "topology already matches spec")
			logger.Info("completed plan still matches spec", "planID", active.ID)
			res, err := r.finish(ctx, cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
			return res, err, true
		}
		cluster.Status.ActivePlan = nil
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "Replanning", "topology drifted")
		setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", "existing plan no longer valid")
		logger.Info("completed plan drifted, replanning", "planID", active.ID)
		res, err := r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
		return res, err, true
	case plan.PlanStateFailed:
		if topologyMatchesSpec(cluster.Status.Topology, spec) {
			r.event(cluster, "PlanFailedCleared", failedPlanMessage(active))
			cluster.Status.ActivePlan = nil
			markNoPlanNeeded(cluster, "topology already matches spec")
			logger.Info("failed plan cleared because topology matches spec", "planID", active.ID)
			res, err := r.finish(ctx, cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
			return res, err, true
		}
		r.event(cluster, "PlanFailedCleared", failedPlanMessage(active))
		cluster.Status.ActivePlan = nil
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "Replanning", "topology drifted")
		setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", "existing plan no longer valid")
		logger.Info("failed plan cleared, replanning", "planID", active.ID)
		res, err := r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
		return res, err, true
	default:
		logger.Info("no work this reconcile")
		res, err := r.finish(ctx, cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
		return res, err, true
	}
}

func (r *RedisClusterReconciler) reconcilePlanIfStillNeeded(ctx context.Context, cluster *v1alpha1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx)
	var latest v1alpha1.RedisCluster
	if err := r.APIReader.Get(ctx, client.ObjectKeyFromObject(cluster), &latest); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("rediscluster not found before planning")
			return ctrl.Result{}, nil
		}
		logger.Error(err, "fresh rediscluster get before planning failed")
		return ctrl.Result{}, err
	}
	spec := toClusterSpec(&latest)
	if res, err, handled := r.handleActivePlan(ctx, &latest, spec); handled {
		logger.Info("fresh active plan found before planning")
		return res, err
	}
	logger.Info("fresh read confirmed no active plan, reconciling plan")
	return r.reconcilePlan(ctx, cluster, toClusterSpec(cluster))
}

func (r *RedisClusterReconciler) reconcilePlan(ctx context.Context, cluster *v1alpha1.RedisCluster, spec plan.ClusterSpec) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)

	logger.Info("planning started")
	req := toPlannerRequest(cluster, spec)
	validationRetries := r.PlanValidationRetries
	if validationRetries < 0 {
		validationRetries = 0
	}
	for attempt := 0; attempt <= validationRetries; attempt++ {
		collectStart := time.Now()
		nodes, err := r.Driver.CollectObservedNodes(ctx, cluster)
		if err != nil {
			logger.Error(err, "collect observed nodes failed", "attempt", attempt, "duration", time.Since(collectStart))
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "ObservedNodesFailed", err.Error())
			cluster.Status.ObservedGeneration = cluster.Generation
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		logger.Info("observed nodes collected", "attempt", attempt, "nodes", len(nodes), "duration", time.Since(collectStart))
		req.ObservedState.Nodes = nodes
		if observedNodesMatchSpec(nodes, spec) {
			cluster.Status.ActivePlan = nil
			markNoPlanNeeded(cluster, "live topology already matches spec")
			logger.Info("live topology already matches spec, skipping planner", "attempt", attempt)
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
		}

		attemptStart := time.Now()
		logger.Info("planner attempt started", "attempt", attempt, "maxAttempts", validationRetries+1, "feedback", len(req.ValidationFeedback))
		newPlan, err := r.Planner.Plan(ctx, req)
		if err != nil {
			logger.Error(err, "planner returned an error", "attempt", attempt, "duration", time.Since(attemptStart))
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerFailed", err.Error())
			cluster.Status.ObservedGeneration = cluster.Generation
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		if newPlan == nil {
			logger.Info("planner returned no plan", "attempt", attempt, "duration", time.Since(attemptStart))
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerEmpty", "planner returned no plan")
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		logger.Info("planner returned plan", "attempt", attempt, "planID", newPlan.PlanID, "steps", len(newPlan.Steps), "duration", time.Since(attemptStart))
		validateStart := time.Now()
		if err := r.ValidatePlan(newPlan, validationContext(cluster, spec, nodes)); err != nil {
			logger.Info("plan rejected", "attempt", attempt, "planID", newPlan.PlanID, "duration", time.Since(validateStart), "error", err)
			if attempt < validationRetries {
				req.ValidationFeedback = append(req.ValidationFeedback, planner.ValidationFeedback{
					RejectedPlan: newPlan,
					Error:        err.Error(),
					Hint:         plan.ValidationHint(err),
				})
				logger.Info("validation feedback appended", "attempt", attempt, "feedback", len(req.ValidationFeedback))
				continue
			}
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanRejected", err.Error())
			cluster.Status.ObservedGeneration = cluster.Generation
			r.event(cluster, "PlanRejected", err.Error())
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		logger.Info("plan validation passed", "attempt", attempt, "planID", newPlan.PlanID, "duration", time.Since(validateStart))
		advanceNextPodOrdinalFromPlan(cluster, newPlan)
		logger.Info("next pod ordinal advanced from plan", "nextPodOrdinal", cluster.Status.NextPodOrdinal)
		cluster.Status.ActivePlan, err = planToStatus(newPlan)
		if err != nil {
			logger.Error(err, "plan persist conversion failed", "planID", newPlan.PlanID)
			setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanPersistFailed", err.Error())
			cluster.Status.ObservedGeneration = cluster.Generation
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		logger.Info("plan accepted", "planID", newPlan.PlanID, "steps", len(newPlan.Steps))
		cluster.Status.ObservedGeneration = cluster.Generation
		setCondition(cluster, ConditionPlanned, metav1.ConditionTrue, "PlanAccepted", "plan passed validation")
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}

	logger.Info("planning finished without accepted plan")
	return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: r.TopologyRefreshInterval}, nil)
}

func markNoPlanNeeded(cluster *v1alpha1.RedisCluster, message string) {
	cluster.Status.ObservedGeneration = cluster.Generation
	setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "NoPlanNeeded", message)
	setCondition(cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "cluster matches desired topology")
}

const maxMigrateBatchSize = 4

func (r *RedisClusterReconciler) applyStepOutcome(ctx context.Context, cluster *v1alpha1.RedisCluster, active *v1alpha1.PlanStatus, planModel *plan.Plan, idx int, outcome StepOutcome, err error) bool {
	logger := log.FromContext(ctx).WithValues("stepIndex", idx, "stepID", active.Steps[idx].ID)
	if err != nil {
		logger.Error(err, "step execution failed", "message", outcome.Message)
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		return true
	}
	if planModel.Steps[idx].Params != nil {
		raw, merr := json.Marshal(planModel.Steps[idx].Params)
		if merr != nil {
			logger.Error(merr, "step params marshal failed")
			setStep(active, idx, string(plan.StepStateFailed), merr.Error())
			return true
		}
		active.Steps[idx].Params = apiextensionsv1.JSON{Raw: raw}
	}
	setStep(active, idx, string(outcome.Status), outcome.Message)
	return outcome.Status == plan.StepStateFailed
}

func (r *RedisClusterReconciler) executeNextStep(ctx context.Context, cluster *v1alpha1.RedisCluster, active *v1alpha1.PlanStatus) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("planID", active.ID)
	idx := nextPendingStep(active)
	if idx < 0 {
		logger.Info("no pending step found")
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 5 * time.Second}, nil)
	}

	step := active.Steps[idx]
	logger = logger.WithValues("stepIndex", idx, "stepID", step.ID, "action", step.Action)
	logger.Info("step execution started")
	restoreStart := time.Now()
	planModel, err := statusToPlan(active)
	if err != nil {
		logger.Error(err, "active plan restore failed", "duration", time.Since(restoreStart))
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		active.CurrentStep = step.ID
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "PlanRestoreFailed", err.Error())
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}
	logger.Info("active plan restored", "duration", time.Since(restoreStart))
	if step.Action == string(plan.ActionMigrateSlots) {
		return r.executeMigrateStepBatch(ctx, cluster, active, planModel, idx)
	}
	execStart := time.Now()
	outcome, err := r.Driver.ExecuteStep(ctx, cluster, planModel, idx)
	logger.Info("step execution finished", "duration", time.Since(execStart), "status", outcome.Status, "message", outcome.Message)
	failed := r.applyStepOutcome(ctx, cluster, active, planModel, idx, outcome, err)
	active.CurrentStep = step.ID
	if failed {
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", active.Steps[idx].Message)
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}
	switch outcome.Status {
	case plan.StepStateCompleted:
		logger.Info("step completed")
		return r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
	case plan.StepStateRunning:
		logger.Info("step still running", "message", outcome.Message)
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: time.Second}, nil)
	default:
		logger.Info("step returned unknown status", "status", outcome.Status)
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: time.Second}, nil)
	}
}

type stepExecutionResult struct {
	index   int
	outcome StepOutcome
	err     error
}

func (r *RedisClusterReconciler) executeMigrateStepBatch(ctx context.Context, cluster *v1alpha1.RedisCluster, active *v1alpha1.PlanStatus, planModel *plan.Plan, start int) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("planID", active.ID)
	indices := independentMigrateStepBatch(active, planModel, start)
	if len(indices) == 0 {
		indices = []int{start}
	}

	batchCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	results := make(chan stepExecutionResult, len(indices))
	var wg sync.WaitGroup
	for _, idx := range indices {
		wg.Add(1)
		go func(idx int) {
			defer wg.Done()
			execStart := time.Now()
			outcome, err := r.Driver.ExecuteStep(batchCtx, cluster, planModel, idx)
			log.FromContext(ctx).WithValues("stepIndex", idx, "stepID", active.Steps[idx].ID, "action", active.Steps[idx].Action).Info("step execution finished", "duration", time.Since(execStart), "status", outcome.Status, "message", outcome.Message, "error", err)
			results <- stepExecutionResult{index: idx, outcome: outcome, err: err}
		}(idx)
	}
	wg.Wait()
	close(results)

	failed := ""
	running := false
	for result := range results {
		if r.applyStepOutcome(ctx, cluster, active, planModel, result.index, result.outcome, result.err) {
			failed = active.Steps[result.index].Message
			cancel()
		} else if result.outcome.Status == plan.StepStateRunning {
			running = true
		}
	}
	active.CurrentStep = firstPendingID(active.Steps)
	if failed != "" {
		logger.Info("migrate step batch failed", "steps", len(indices), "message", failed)
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", failed)
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}
	if running {
		logger.Info("migrate step batch still running", "steps", len(indices))
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: time.Second}, nil)
	}
	logger.Info("migrate step batch completed", "steps", len(indices))
	return r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
}

func independentMigrateStepBatch(active *v1alpha1.PlanStatus, p *plan.Plan, start int) []int {
	indices := []int{}
	sources := map[string]bool{}
	targets := map[string]bool{}
	for i := start; i < len(p.Steps); i++ {
		if p.Steps[i].Action != plan.ActionMigrateSlots || active.Steps[i].Action != string(plan.ActionMigrateSlots) {
			break
		}
		if active.Steps[i].Status == string(plan.StepStateCompleted) {
			continue
		}
		source, sourceOK := paramString(p.Steps[i].Params, "sourcePod")
		target, targetOK := paramString(p.Steps[i].Params, "targetPod")
		if !sourceOK || !targetOK {
			break
		}
		if sources[source] || targets[target] {
			continue
		}
		sources[source] = true
		targets[target] = true
		indices = append(indices, i)
		if len(indices) >= maxMigrateBatchSize {
			break
		}
	}
	return indices
}

func (r *RedisClusterReconciler) ensureNamespace(ctx context.Context, cluster *v1alpha1.RedisCluster) error {
	logger := log.FromContext(ctx).WithValues("namespace", cluster.Name)
	var ns corev1.Namespace
	err := r.Get(ctx, client.ObjectKey{Name: cluster.Name}, &ns)
	if err == nil {
		logger.Info("namespace already exists")
		return nil
	}
	if !apierrors.IsNotFound(err) {
		logger.Error(err, "get namespace failed")
		return err
	}
	logger.Info("creating namespace")
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
	if err := r.Create(ctx, nsObj); err != nil {
		logger.Error(err, "create namespace failed")
		return err
	}
	logger.Info("namespace created")
	return nil
}

func (r *RedisClusterReconciler) reconcileDelete(ctx context.Context, cluster *v1alpha1.RedisCluster) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)
	logger.Info("delete reconcile started")
	if !controllerutil.ContainsFinalizer(cluster, finalizer) {
		logger.Info("finalizer already absent")
		return ctrl.Result{}, nil
	}
	var ns corev1.Namespace
	err := r.Get(ctx, client.ObjectKey{Name: cluster.Name}, &ns)
	if err == nil {
		if ns.DeletionTimestamp.IsZero() {
			logger.Info("deleting owned namespace", "namespace", cluster.Name)
			if derr := r.Delete(ctx, &ns); derr != nil && !apierrors.IsNotFound(derr) {
				logger.Error(derr, "delete namespace failed", "namespace", cluster.Name)
				return ctrl.Result{}, derr
			}
		}
		logger.Info("waiting for namespace deletion", "namespace", cluster.Name)
		return ctrl.Result{RequeueAfter: 5 * time.Second}, nil
	}
	if !apierrors.IsNotFound(err) {
		logger.Error(err, "get namespace during delete failed", "namespace", cluster.Name)
		return ctrl.Result{}, err
	}
	logger.Info("owned namespace gone, removing finalizer", "namespace", cluster.Name)
	controllerutil.RemoveFinalizer(cluster, finalizer)
	if err := r.Update(ctx, cluster); err != nil {
		logger.Error(err, "remove finalizer failed")
		return ctrl.Result{}, err
	}
	logger.Info("finalizer removed")
	return ctrl.Result{}, nil
}

func (r *RedisClusterReconciler) finish(ctx context.Context, cluster *v1alpha1.RedisCluster, res ctrl.Result, err error) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)
	start := time.Now()
	logger.Info("status update started", "requeue", res.Requeue, "requeueAfter", res.RequeueAfter, "incomingError", err)
	if uerr := r.Status().Update(ctx, cluster); uerr != nil && err == nil {
		if apierrors.IsConflict(uerr) {
			logger.Info("status update conflict", "duration", time.Since(start), "requeueAfter", statusConflictRequeueAfter)
			return ctrl.Result{RequeueAfter: statusConflictRequeueAfter}, nil
		}
		logger.Error(uerr, "status update failed", "duration", time.Since(start))
		return res, uerr
	}
	logger.Info("status update finished", "duration", time.Since(start), "skippedError", err != nil)
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
