package controller

import (
	"encoding/json"
	"fmt"

	apiextensionsv1 "k8s.io/apiextensions-apiserver/pkg/apis/apiextensions/v1"
	"k8s.io/apimachinery/pkg/api/meta"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

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
