package planner

import (
	"context"
	"encoding/json"
	"fmt"

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
		MaxTokens:   0,
		Temperature: 0,
	}
}

// Plan implements Planner.
func (p *LLMPlanner) Plan(ctx context.Context, req Request) (*plan.Plan, error) {
	if p.Client == nil {
		return nil, fmt.Errorf("llm planner: client is nil")
	}

	llmReq := llm.Request{
		Model:  p.Model,
		System: buildSystemPrompt(),
		Messages: []llm.Message{
			{
				Role: llm.RoleUser,
				Content: []llm.ContentPart{
					{Type: "text", Text: buildUserPrompt(req)},
				},
			},
		},
		ResponseFormat: llm.ResponseFormat{
			Type: llm.ResponseFormatJSONObject,
		},
		MaxTokens:       p.MaxTokens,
		Temperature:     p.Temperature,
		ReasoningEffort: p.ReasoningEffort,
	}

	resp, err := p.Client.Complete(ctx, llmReq)
	if err != nil {
		return nil, fmt.Errorf("llm complete: %w", err)
	}
	if resp == nil || resp.Text == "" {
		return nil, fmt.Errorf("llm returned empty response")
	}

	var p2 plan.Plan
	if err := json.Unmarshal([]byte(resp.Text), &p2); err != nil {
		return nil, fmt.Errorf("unmarshal plan json: %w (response: %s)", err, truncate(resp.Text, 500))
	}

	p2.DSLVersion = plan.DSLVersion
	p2.TargetGeneration = req.Spec.Generation
	return &p2, nil
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
)
