package planner

import "context"

type LLMClient interface {
	Complete(ctx context.Context, req LLMRequest) (*LLMResponse, error)
}

type LLMRequest struct {
	System string
	Prompt string
}

type LLMResponse struct {
	Text string
}
