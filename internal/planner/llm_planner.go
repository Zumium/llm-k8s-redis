package planner

import (
	"context"

	"github.com/example/llm-k8s-redis/internal/llm"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// LLMPlanner is a Planner implementation that delegates plan generation to an
// LLM via the provider-agnostic llm.Client interface.
//
// Flow:
//  1. Build system + user prompts from the Request.
//  2. Call llm.Client.Complete with a JSON response format.
//  3. Unmarshal the response text into a plan.Plan.
//  4. Fix up DSLVersion and TargetGeneration so the plan is always consistent
//     with the controller's expectations, regardless of what the LLM emitted.
//  5. Return the plan. The controller's Validator is the final safety gate.
type LLMPlanner struct {
	Client          llm.Client
	Model           string
	MaxTokens       int
	Temperature     float32
	ReasoningEffort string
}

// NewLLMPlanner returns an LLMPlanner with sensible defaults.
func NewLLMPlanner(client llm.Client, model string) *LLMPlanner {
	return &LLMPlanner{
		Client:      client,
		Model:       model,
		MaxTokens:   8192,
		Temperature: 0,
	}
}

// Plan implements Planner.
func (p *LLMPlanner) Plan(ctx context.Context, req Request) (*plan.Plan, error) {
	return planWithClient(ctx, p.Client, p.Model, p.MaxTokens, p.Temperature, p.ReasoningEffort, req)
}

func truncate(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

// compile-time checks that both planners satisfy Planner.
var (
	_ Planner = (*LLMPlanner)(nil)
	_ Planner = (*DynamicPlanner)(nil)
)
