package planner

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

type LLMPlanner struct {
	LLMClient LLMClient
}

func NewLLMPlanner(llmClient LLMClient) *LLMPlanner {
	return &LLMPlanner{LLMClient: llmClient}
}

func (p *LLMPlanner) Plan(ctx context.Context, req Request) (*plan.Plan, error) {
	if p.LLMClient == nil {
		return nil, fmt.Errorf("llm planner: client is nil")
	}

	planJSON, err := p.askLLMForPlanJSON(ctx, req)
	if err != nil {
		return nil, err
	}
	generatedPlan, err := parsePlanJSON(planJSON)
	if err != nil {
		return nil, err
	}
	setControllerOwnedFields(generatedPlan, req)
	return generatedPlan, nil
}

func (p *LLMPlanner) askLLMForPlanJSON(ctx context.Context, req Request) (string, error) {
	llmReq, err := buildLLMRequest(req)
	if err != nil {
		return "", err
	}
	response, err := p.LLMClient.Complete(ctx, llmReq)
	if err != nil {
		return "", fmt.Errorf("llm complete: %w", err)
	}
	if response == nil || response.Text == "" {
		return "", fmt.Errorf("llm returned empty response")
	}
	return response.Text, nil
}

func buildLLMRequest(req Request) (LLMRequest, error) {
	system := buildSystemPrompt()
	prompt := buildUserPrompt(req)
	messages := []LLMMessage{
		{Role: "system", Content: system},
		{Role: "user", Content: prompt},
	}
	for _, feedback := range req.ValidationFeedback {
		rejectedPlan, err := json.Marshal(feedback.RejectedPlan)
		if err != nil {
			return LLMRequest{}, fmt.Errorf("marshal rejected plan: %w", err)
		}
		messages = append(messages,
			LLMMessage{Role: "assistant", Content: string(rejectedPlan)},
			LLMMessage{Role: "user", Content: buildValidationFeedbackPrompt(feedback)},
		)
	}
	return LLMRequest{Messages: messages}, nil
}

func buildValidationFeedbackPrompt(feedback ValidationFeedback) string {
	if feedback.Hint != "" {
		return fmt.Sprintf("The controller Validator rejected that plan:\n%s\nFix: %s\nReturn a corrected JSON plan only.", feedback.Error, feedback.Hint)
	}
	return fmt.Sprintf("The controller Validator rejected that plan:\n%s\nReturn a corrected JSON plan only.", feedback.Error)
}

func parsePlanJSON(planJSON string) (*plan.Plan, error) {
	var generatedPlan plan.Plan
	if err := json.Unmarshal([]byte(planJSON), &generatedPlan); err != nil {
		return nil, fmt.Errorf("unmarshal plan json: %w (response: %s)", err, shortenForError(planJSON, 500))
	}
	return &generatedPlan, nil
}

func setControllerOwnedFields(generatedPlan *plan.Plan, req Request) {
	generatedPlan.DSLVersion = plan.DSLVersion
	generatedPlan.TargetGeneration = req.Spec.Generation
}

func shortenForError(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "..."
}

var _ Planner = (*LLMPlanner)(nil)
