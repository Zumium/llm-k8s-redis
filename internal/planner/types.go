package planner

import (
	"context"
	"errors"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

type Planner interface {
	Plan(ctx context.Context, req Request) (*plan.Plan, error)
}

type Request struct {
	Spec               plan.ClusterSpec
	ObservedState      ObservedState
	ValidationFeedback []ValidationFeedback
}

type ObservedState struct {
	ActivePlan     *v1alpha1.PlanStatus
	NextPodOrdinal int
	Nodes          []ObservedNode
}

type ObservedNode = plan.ObservedNode

type ValidationFeedback struct {
	RejectedPlan *plan.Plan
	Error        string
}

var ErrNotConfigured = errors.New("planner not configured; provide a Planner implementation")

type NoopPlanner struct{}

func (NoopPlanner) Plan(_ context.Context, _ Request) (*plan.Plan, error) {
	return nil, ErrNotConfigured
}
