package controller

import (
	"context"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/planner"
)

type Driver interface {
	ExecuteStep(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error)
	ObserveTopology(ctx context.Context, cluster *v1alpha1.RedisCluster) error
	CollectObservedNodes(ctx context.Context, cluster *v1alpha1.RedisCluster) ([]plan.ObservedNode, error)
}

type StepOutcome struct {
	Status  plan.StepState
	Message string
}

var _ planner.Planner = planner.NoopPlanner{}
