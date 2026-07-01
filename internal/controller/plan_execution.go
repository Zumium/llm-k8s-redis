package controller

import (
	"context"
	"time"

	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

func (r *RedisClusterReconciler) handleActivePlan(ctx context.Context, cluster *v1alpha1.RedisCluster) (ctrl.Result, error, bool) {
	logger := log.FromContext(ctx)
	active := cluster.Status.ActivePlan
	if active == nil {
		return ctrl.Result{}, nil, false
	}
	state := planState(active)
	if active.TargetGeneration != cluster.Generation {
		logger.Info("active plan cleared by generation", "planID", active.ID, "targetGeneration", active.TargetGeneration, "generation", cluster.Generation)
		return r.clearActivePlanAndRequeue(ctx, cluster, "active plan targets an older generation", "active plan targets an older generation")
	}
	if state == plan.PlanStateRunning {
		logger.Info("running active plan found", "planID", active.ID, "steps", len(active.Steps), "currentStep", active.CurrentStep)
		res, err := r.executeNextStep(ctx, cluster, active)
		return res, err, true
	}

	logger.Info("active plan found", "planID", active.ID, "planState", state, "targetGeneration", active.TargetGeneration, "steps", len(active.Steps))
	switch state {
	case plan.PlanStateCompleted:
		cluster.Status.ActivePlan = nil
		setCondition(cluster, ConditionPlanned, metav1.ConditionTrue, "PlanCompleted", "active plan completed")
		res, err := r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
		return res, err, true
	case plan.PlanStateFailed:
		active.Status = string(plan.PlanStateFailed)
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "PlanFailed", failedPlanMessage(active))
		logger.Info("failed plan retained", "planID", active.ID)
		res, err := r.finish(ctx, cluster, ctrl.Result{}, nil)
		return res, err, true
	case plan.PlanStateSuperseded:
		cluster.Status.ActivePlan = nil
		res, err := r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
		return res, err, true
	default:
		logger.Info("no work this reconcile")
		res, err := r.finish(ctx, cluster, ctrl.Result{}, nil)
		return res, err, true
	}
}

func (r *RedisClusterReconciler) clearActivePlanAndRequeue(ctx context.Context, cluster *v1alpha1.RedisCluster, readyMessage, plannedMessage string) (ctrl.Result, error, bool) {
	cluster.Status.ActivePlan = nil
	setCondition(cluster, ConditionReady, metav1.ConditionFalse, "Replanning", readyMessage)
	setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", plannedMessage)
	res, err := r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
	return res, err, true
}

func (r *RedisClusterReconciler) executeNextStep(ctx context.Context, cluster *v1alpha1.RedisCluster, active *v1alpha1.PlanStatus) (ctrl.Result, error) {
	logger := log.FromContext(ctx).WithValues("planID", active.ID)
	idx := nextPendingStep(active)
	if idx < 0 {
		logger.Info("no pending step found")
		active.Status = string(plan.PlanStateCompleted)
		return r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
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

	execStart := time.Now()
	outcome, err := r.Driver.ExecuteStep(ctx, cluster, planModel, idx)
	logger.Info("step execution finished", "duration", time.Since(execStart), "status", outcome.Status, "message", outcome.Message)
	if err == nil && outcome.SupersedePlan {
		return r.supersedeActivePlan(ctx, cluster, active, planModel, idx, step.ID, outcome.Message)
	}

	failed := r.applyStepOutcome(ctx, cluster, active, planModel, idx, outcome, err)
	active.CurrentStep = step.ID
	if failed {
		active.Status = string(plan.PlanStateFailed)
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", active.Steps[idx].Message)
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}

	switch outcome.Status {
	case plan.StepStateCompleted:
		if nextPendingStep(active) < 0 {
			active.Status = string(plan.PlanStateCompleted)
		}
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

func (r *RedisClusterReconciler) supersedeActivePlan(ctx context.Context, cluster *v1alpha1.RedisCluster, active *v1alpha1.PlanStatus, planModel *plan.Plan, idx int, stepID, message string) (ctrl.Result, error) {
	if err := persistStepParams(active, planModel, idx); err != nil {
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		active.Status = string(plan.PlanStateFailed)
		setCondition(cluster, ConditionReady, metav1.ConditionFalse, "StepFailed", err.Error())
		return r.finish(ctx, cluster, ctrl.Result{}, nil)
	}
	setStep(active, idx, string(plan.StepStateRunning), message)
	active.CurrentStep = stepID
	active.Status = string(plan.PlanStateSuperseded)
	setCondition(cluster, ConditionReady, metav1.ConditionFalse, "Replanning", message)
	setCondition(cluster, ConditionPlanned, metav1.ConditionFalse, "PlanSuperseded", message)
	return r.finish(ctx, cluster, ctrl.Result{Requeue: true}, nil)
}

func (r *RedisClusterReconciler) applyStepOutcome(ctx context.Context, cluster *v1alpha1.RedisCluster, active *v1alpha1.PlanStatus, planModel *plan.Plan, idx int, outcome StepOutcome, err error) bool {
	logger := log.FromContext(ctx).WithValues("stepIndex", idx, "stepID", active.Steps[idx].ID)
	if err != nil {
		logger.Error(err, "step execution failed", "message", outcome.Message)
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		return true
	}
	if err := persistStepParams(active, planModel, idx); err != nil {
		logger.Error(err, "step params marshal failed")
		setStep(active, idx, string(plan.StepStateFailed), err.Error())
		return true
	}
	setStep(active, idx, string(outcome.Status), outcome.Message)
	return outcome.Status == plan.StepStateFailed
}

func persistStepParams(active *v1alpha1.PlanStatus, planModel *plan.Plan, idx int) error {
	if planModel.Steps[idx].Params == nil {
		return nil
	}
	converted, err := planToStatus(planModel)
	if err != nil {
		return err
	}
	active.Steps[idx].Params = converted.Steps[idx].Params
	return nil
}
