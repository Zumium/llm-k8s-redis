package planner

import "context"

type LLMClient interface {
	Complete(ctx context.Context, req LLMRequest) (*LLMResponse, error)
}

type LLMRequest struct {
	Messages []LLMMessage
}

type LLMResponse struct {
	Text string
}

type LLMMessage struct {
	Role    string
	Content string
}
