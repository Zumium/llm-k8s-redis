package planner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/example/llm-k8s-redis/internal/llm"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// PlannerSource provides the current llm.Client and model name. It decouples
// the planner from where the LLM config comes from (ConfigMap, flags, etc.).
type PlannerSource interface {
	Get() (llm.Config, llm.Client, error)
}

// DynamicPlanner is a Planner that resolves the LLM client and model on every
// call from a PlannerSource. This makes it hot-reload compatible: when the
// ConfigMap changes, the next Plan() call uses the new client/model without
// restarting the controller.
type DynamicPlanner struct {
	Source          PlannerSource
	MaxTokens       int
	Temperature     float32
	ReasoningEffort string
}

// NewDynamicPlanner returns a DynamicPlanner backed by the given source.
func NewDynamicPlanner(src PlannerSource) *DynamicPlanner {
	return &DynamicPlanner{
		Source:      src,
		MaxTokens:   8192,
		Temperature: 0,
	}
}

// Plan implements Planner.
func (p *DynamicPlanner) Plan(ctx context.Context, req Request) (*plan.Plan, error) {
	if p.Source == nil {
		return nil, fmt.Errorf("dynamic planner: source is nil")
	}
	cfg, client, err := p.Source.Get()
	if err != nil {
		return nil, fmt.Errorf("dynamic planner: %w", err)
	}
	if client == nil {
		return nil, fmt.Errorf("dynamic planner: llm client is nil")
	}
	lp := LLMPlanner{
		Client:          client,
		Model:           cfg.Model,
		MaxTokens:       orInt(p.MaxTokens, cfg.MaxTokens),
		Temperature:     orFloat(p.Temperature, cfg.Temperature),
		ReasoningEffort: orString(p.ReasoningEffort, cfg.ReasoningEffort),
	}
	return lp.Plan(ctx, req)
}

func orInt(a, b int) int {
	if a != 0 {
		return a
	}
	return b
}

func orFloat(a, b float32) float32 {
	if a != 0 {
		return a
	}
	return b
}

func orString(a, b string) string {
	if a != "" {
		return a
	}
	return b
}

// planWithClient contains the shared prompt-build → call → parse → fixup logic
// used by both the static LLMPlanner and any future planner that injects a
// client per call.
func planWithClient(ctx context.Context, client llm.Client, model string, maxTokens int, temperature float32, reasoningEffort string, req Request) (*plan.Plan, error) {
	if client == nil {
		return nil, fmt.Errorf("llm planner: client is nil")
	}

	llmReq := llm.Request{
		Model:  model,
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
		MaxTokens:       maxTokens,
		Temperature:     temperature,
		ReasoningEffort: reasoningEffort,
	}

	resp, err := client.Complete(ctx, llmReq)
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

	// Post-process: enforce fields the LLM might get wrong. The Validator
	// still checks them, but fixing them here avoids spurious rejections.
	p2.DSLVersion = plan.DSLVersion
	p2.TargetGeneration = req.Spec.Generation
	if string(p2.Operation) == "" {
		p2.Operation = req.Operation
	}
	return &p2, nil
}
