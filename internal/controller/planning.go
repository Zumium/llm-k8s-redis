package controller

import (
	"context"
	"time"

	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func (r *RedisClusterReconciler) reconcilePlan(ctx context.Context, cluster *v1alpha1.RedisCluster, spec plan.ClusterSpec) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)
	logger.Info("planning started")

	req := toPlannerRequest(cluster, spec)
	collectStart := time.Now()
	nodes, err := r.Driver.CollectObservedNodes(ctx, cluster)
	if err != nil {
		logger.Error(err, "collect observed nodes failed", "duration", time.Since(collectStart))
		setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "ObservedNodesFailed", err.Error())
		cluster.Status.ObservedGeneration = cluster.Generation
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
	}
	logger.Info("observed nodes collected", "nodes", len(nodes), "duration", time.Since(collectStart))

	observed := apiObservedNodes(nodes)
	previousObserved := cluster.Status.ObservedNodes
	cluster.Status.ObservedNodes = observed
	bumpNextPodOrdinalFromObserved(cluster, nodes)
	req.ObservedState.Nodes = nodes
	req.ObservedState.NextPodOrdinal = int(cluster.Status.NextPodOrdinal)

	if observedNodesMatchSpec(nodes, spec) {
		cluster.Status.ActivePlan = nil
		markNoPlanNeeded(cluster, "live topology already matches spec")
		logger.Info("live topology already matches spec, skipping planner")
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}
	if reason := planningWaitReason(cluster, previousObserved, observed, nodes, spec); reason != "" {
		if !clusterStableWaitTimedOut(cluster) {
			markWaitingForStableCluster(cluster, reason)
			logger.Info("waiting for cluster to stabilize before planning", "reason", reason)
			return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
		}
		logger.Info("cluster stability wait timed out, planning repair", "reason", reason)
	}

	attemptStart := time.Now()
	logger.Info("planner attempt started")
	newPlan, err := r.Planner.Plan(ctx, req)
	if err != nil {
		logger.Error(err, "planner returned an error", "duration", time.Since(attemptStart))
		setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerFailed", err.Error())
		cluster.Status.ObservedGeneration = cluster.Generation
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
	}
	if newPlan == nil {
		logger.Info("planner returned no plan", "duration", time.Since(attemptStart))
		setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlannerEmpty", "planner returned no plan")
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
	}
	logger.Info("planner returned plan", "planID", newPlan.PlanID, "steps", len(newPlan.Steps), "duration", time.Since(attemptStart))

	validateStart := time.Now()
	if err := r.ValidatePlan(spec, nodes, newPlan); err != nil {
		logger.Info("plan rejected", "planID", newPlan.PlanID, "duration", time.Since(validateStart), "error", err)
		setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanRejected", err.Error())
		cluster.Status.ObservedGeneration = cluster.Generation
		r.event(cluster, "PlanRejected", err.Error())
		return r.finish(ctx, cluster, ctrl.Result{RequeueAfter: 10 * time.Second}, nil)
	}
	logger.Info("plan validation passed", "planID", newPlan.PlanID, "duration", time.Since(validateStart))

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

func planningWaitReason(cluster *v1alpha1.RedisCluster, previousObserved, observed []v1alpha1.ObservedNode, nodes []plan.ObservedNode, spec plan.ClusterSpec) string {
	reason := planningInstabilityReason(nodes, spec)
	observedStable := observedStatusNodesEqual(previousObserved, observed) || (len(previousObserved) == 0 && len(observed) == 0 && clusterStableWaitStarted(cluster))
	if !observedStable && reason == "" {
		return "waiting for stable observed nodes before planning"
	}
	return reason
}

func markWaitingForStableCluster(cluster *v1alpha1.RedisCluster, reason string) {
	old := meta.FindStatusCondition(cluster.Status.Conditions, ConditionPlanned)
	setCondition(cluster, ConditionReady, metav1.ConditionFalse, "WaitingForClusterStable", reason)
	setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "WaitingForClusterStable", reason)
	if old != nil && old.Reason != "WaitingForClusterStable" {
		meta.FindStatusCondition(cluster.Status.Conditions, ConditionPlanned).LastTransitionTime = metav1.Now()
	}
	cluster.Status.ObservedGeneration = cluster.Generation
}

func markNoPlanNeeded(cluster *v1alpha1.RedisCluster, message string) {
	cluster.Status.ObservedGeneration = cluster.Generation
	setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "NoPlanNeeded", message)
	setCondition(cluster, ConditionReady, metav1.ConditionTrue, "ClusterReady", "cluster matches desired topology")
}
