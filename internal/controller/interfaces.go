package controller

import (
	"context"
	"errors"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// Planner produces a Plan for the given RedisCluster. Implementations wrap an
// LLM call but must return a plan that the Validator can accept; the
// controller never trusts the plan without validation.
type Planner interface {
	Plan(ctx context.Context, cluster *v1alpha1.RedisCluster, spec plan.ClusterSpec) (*plan.Plan, error)
}

// Executor runs a single plan step. It must be idempotent and re-derive state
// from the cluster on every call rather than relying on in-memory state.
type Executor interface {
	ExecuteStep(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error)
}

// Validator is the safety gate between planner and executor.
type Validator interface {
	Validate(p *plan.Plan, spec plan.ClusterSpec) error
}

// StepOutcome is the result of executing one step.
type StepOutcome struct {
	Status  plan.StepState
	Message string
}

// ErrPlannerNotConfigured is returned by NoopPlanner.
var ErrPlannerNotConfigured = errors.New("planner not configured; provide a Planner implementation")

// NoopPlanner is the default stub used until the LLM planner is wired in.
type NoopPlanner struct{}

func (NoopPlanner) Plan(_ context.Context, _ *v1alpha1.RedisCluster, _ plan.ClusterSpec) (*plan.Plan, error) {
	return nil, ErrPlannerNotConfigured
}

// ErrExecutorNotConfigured is returned by NoopExecutor.
var ErrExecutorNotConfigured = errors.New("executor not configured; provide an Executor implementation")

// NoopExecutor is the default stub used until Redis action executors are wired in.
type NoopExecutor struct{}

func (NoopExecutor) ExecuteStep(_ context.Context, _ *v1alpha1.RedisCluster, _ *plan.Plan, _ int) (StepOutcome, error) {
	return StepOutcome{Status: plan.StepStateFailed, Message: ErrExecutorNotConfigured.Error()}, ErrExecutorNotConfigured
}
