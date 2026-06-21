package planner

import (
	"context"
	"fmt"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/shared"
)

type OpenAIClient struct {
	client openai.Client
	config Config
}

func NewOpenAIClient(config Config) (*OpenAIClient, error) {
	if err := config.Validate(); err != nil {
		return nil, err
	}
	client := openai.NewClient(
		option.WithAPIKey(config.APIKey),
		option.WithBaseURL(config.BaseURL),
	)
	return &OpenAIClient{client: client, config: config}, nil
}

func (c *OpenAIClient) Complete(ctx context.Context, req LLMRequest) (*LLMResponse, error) {
	completion, err := c.client.Chat.Completions.New(ctx, c.newChatCompletionParams(req))
	if err != nil {
		return nil, fmt.Errorf("openai chat completion: %w", err)
	}
	text, err := firstChoiceText(completion)
	if err != nil {
		return nil, err
	}
	return &LLMResponse{Text: text}, nil
}

func (c *OpenAIClient) newChatCompletionParams(req LLMRequest) openai.ChatCompletionNewParams {
	params := openai.ChatCompletionNewParams{
		Model: shared.ChatModel(c.config.Model),
		Messages: []openai.ChatCompletionMessageParamUnion{
			openai.SystemMessage(req.System),
			openai.UserMessage(req.Prompt),
		},
		ResponseFormat: openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONObject: &shared.ResponseFormatJSONObjectParam{},
		},
	}
	if c.config.MaxTokens > 0 {
		params.MaxTokens = openai.Int(int64(c.config.MaxTokens))
	}
	if c.config.Temperature > 0 {
		params.Temperature = openai.Float(float64(c.config.Temperature))
	}
	if c.config.ReasoningEffort != "" {
		params.ReasoningEffort = shared.ReasoningEffort(c.config.ReasoningEffort)
	}
	return params
}

func firstChoiceText(completion *openai.ChatCompletion) (string, error) {
	if len(completion.Choices) == 0 {
		return "", fmt.Errorf("openai response had no choices")
	}
	return completion.Choices[0].Message.Content, nil
}
