package planner

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

type LLMPlanner struct {
	LLMClient LLMClient
}

const llmPlanTimeout = 2 * time.Minute

func NewLLMPlanner(llmClient LLMClient) *LLMPlanner {
	return &LLMPlanner{LLMClient: llmClient}
}

func (p *LLMPlanner) Plan(ctx context.Context, req Request) (*plan.Plan, error) {
	if p.LLMClient == nil {
		return nil, fmt.Errorf("llm planner: client is nil")
	}
	ctx, cancel := context.WithTimeout(ctx, llmPlanTimeout)
	defer cancel()

	logger := log.FromContext(ctx)
	start := time.Now()
	logger.Info("llm analysis request started")
	analysisJSON, err := p.askLLMForAnalysisJSON(ctx, req)
	if err != nil {
		logger.Error(err, "llm analysis request failed", "duration", time.Since(start))
		return nil, err
	}
	logger.Info("llm analysis request finished", "duration", time.Since(start))

	start = time.Now()
	logger.Info("llm plan request started")
	planJSON, err := p.askLLMForPlanJSON(ctx, req, analysisJSON)
	if err != nil {
		logger.Error(err, "llm plan request failed", "duration", time.Since(start))
		return nil, err
	}
	logger.Info("llm plan request finished", "duration", time.Since(start))
	generatedPlan, err := parsePlanJSON(planJSON)
	if err != nil {
		return nil, err
	}
	setControllerOwnedFields(generatedPlan, req)
	logger.Info("planner produced plan", "planner", "llm", "planID", generatedPlan.PlanID, "steps", len(generatedPlan.Steps), "targetGeneration", generatedPlan.TargetGeneration)
	return generatedPlan, nil
}

func (p *LLMPlanner) askLLMForAnalysisJSON(ctx context.Context, req Request) (string, error) {
	return p.complete(ctx, buildAnalysisLLMRequest(req))
}

func (p *LLMPlanner) askLLMForPlanJSON(ctx context.Context, req Request, analysisJSON string) (string, error) {
	llmReq, err := buildLLMRequest(req, analysisJSON)
	if err != nil {
		return "", err
	}
	return p.complete(ctx, llmReq)
}

func (p *LLMPlanner) complete(ctx context.Context, req LLMRequest) (string, error) {
	response, err := p.LLMClient.Complete(ctx, req)
	if err != nil {
		return "", fmt.Errorf("llm complete: %w", err)
	}
	if response == nil || response.Text == "" {
		return "", fmt.Errorf("llm returned empty response")
	}
	return response.Text, nil
}

func buildAnalysisLLMRequest(req Request) LLMRequest {
	return LLMRequest{Messages: []LLMMessage{
		{Role: "system", Content: buildSystemPrompt()},
		{Role: "user", Content: buildAnalysisPrompt(req)},
	}}
}

func buildLLMRequest(req Request, analysisJSON string) (LLMRequest, error) {
	system := buildSystemPrompt()
	prompt := buildUserPrompt(req)
	planTask := "Use the subprocess analysis above as planning context. Return only the JSON plan."
	if examples := workedExamplesForAnalysis(analysisJSON); examples != "" {
		planTask += "\n\n## Worked examples\n" + examples
	}
	messages := []LLMMessage{
		{Role: "system", Content: system},
		{Role: "user", Content: prompt},
		{Role: "assistant", Content: analysisJSON},
		{Role: "user", Content: planTask},
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
