package controller

import (
	"context"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/action"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
)

type Driver interface {
	ExecuteStep(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error)
	CollectObservedNodes(ctx context.Context, cluster *v1alpha1.RedisCluster) ([]plan.ObservedNode, error)
}

type StepOutcome = action.StepOutcome

var _ planner.Planner = planner.NoopPlanner{}
