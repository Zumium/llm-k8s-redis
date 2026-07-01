package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/client-go/tools/events"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/controller/controllerutil"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/action"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
	"github.com/Zumium/llm-k8s-redis/internal/validator"
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
	Scheme       *runtime.Scheme
	Planner      planner.Planner
	Driver       Driver
	ValidatePlan func(plan.ClusterSpec, []plan.ObservedNode, *plan.Plan) error
	Recorder     events.EventRecorder
}

func (r *RedisClusterReconciler) Reconcile(ctx context.Context, req ctrl.Request) (res ctrl.Result, err error) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues("rediscluster", req.NamespacedName)
	ctx = log.IntoContext(ctx, logger)
	defer func() {
		logger.Info("reconcile finished", "duration", time.Since(start), "requeue", res.Requeue, "requeueAfter", res.RequeueAfter, "error", err)
	}()
	logger.Info("reconcile started")

	r.initDefaults(ctx)

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

	spec := toClusterSpec(&cluster)
	logger.Info("desired spec converted", "shards", spec.Shards, "replicasPerShard", spec.ReplicasPerShard, "image", spec.Image, "memorySize", spec.MemorySize)

	if res, err, handled := r.handleActivePlan(ctx, &cluster); handled {
		return res, err
	}

	if cluster.Status.ActivePlan == nil {
		logger.Info("no active plan, reconciling plan")
		return r.reconcilePlan(ctx, &cluster, spec)
	}

	logger.Info("no work this reconcile")
	return r.finish(ctx, &cluster, ctrl.Result{}, nil)
}

func (r *RedisClusterReconciler) initDefaults(ctx context.Context) {
	logger := log.FromContext(ctx)
	if r.Planner == nil {
		r.Planner = planner.NoopPlanner{}
		logger.Info("default planner initialized")
	}
	if r.Driver == nil {
		r.Driver = &action.ActionExecutor{Client: r.Client, Scheme: r.Scheme}
		logger.Info("default driver initialized")
	}
	if r.ValidatePlan == nil {
		r.ValidatePlan = validator.Validate
		logger.Info("default plan validator initialized")
	}
}

func (r *RedisClusterReconciler) SetupWithManager(mgr ctrl.Manager) error {
	return ctrl.NewControllerManagedBy(mgr).
		For(&v1alpha1.RedisCluster{}).
		Owns(&corev1.Namespace{}).
		Owns(&corev1.Pod{}).
		Complete(r)
}
