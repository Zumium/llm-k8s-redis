package controller

import (
	"context"
	"errors"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/planner"
)

// Executor runs a single plan step. It must be idempotent and re-derive state
// from the cluster on every call rather than relying on in-memory state.
type Executor interface {
	ExecuteStep(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error)
}

// Validator is the safety gate between planner and executor.
type Validator interface {
	Validate(p *plan.Plan, ctx any) error
}

// StepOutcome is the result of executing one step.
type StepOutcome struct {
	Status  plan.StepState
	Message string
}

// ErrExecutorNotConfigured is returned by NoopExecutor.
var ErrExecutorNotConfigured = errors.New("executor not configured; provide an Executor implementation")

// NoopExecutor is the default stub used until Redis action executors are wired in.
type NoopExecutor struct{}

func (NoopExecutor) ExecuteStep(_ context.Context, _ *v1alpha1.RedisCluster, _ *plan.Plan, _ int) (StepOutcome, error) {
	return StepOutcome{Status: plan.StepStateFailed, Message: ErrExecutorNotConfigured.Error()}, ErrExecutorNotConfigured
}

// compile-time check that planner.NoopPlanner satisfies planner.Planner.
var _ planner.Planner = planner.NoopPlanner{}
