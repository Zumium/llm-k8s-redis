package llm

import (
	"context"
	"fmt"
	"strings"

	"github.com/openai/openai-go/v3"
	"github.com/openai/openai-go/v3/option"
	"github.com/openai/openai-go/v3/shared"
)

// OpenAIClient is an llm.Client backed by the official OpenAI Go SDK
// (github.com/openai/openai-go/v3). It is OpenAI-compatible: any endpoint
// exposing POST {baseURL}/chat/completions works, including Azure OpenAI,
// vLLM, Ollama, and Anthropic's OpenAI-compatible layer.
//
// The underlying openai.Client is configured from a Config (baseUrl, apiKey,
// model). Callers should rebuild the client when the ConfigMap changes — see
// ConfigMapSource.
type OpenAIClient struct {
	client openai.Client
	cfg    Config
}

// NewOpenAIClient builds an OpenAIClient from a validated Config.
func NewOpenAIClient(cfg Config) (*OpenAIClient, error) {
	if err := cfg.Validate(); err != nil {
		return nil, err
	}
	client := openai.NewClient(
		option.WithAPIKey(cfg.APIKey),
		option.WithBaseURL(cfg.BaseURL),
	)
	return &OpenAIClient{client: client, cfg: cfg}, nil
}

// Complete implements llm.Client by calling the Chat Completions API.
func (c *OpenAIClient) Complete(ctx context.Context, req Request) (*Response, error) {
	model := req.Model
	if model == "" {
		model = c.cfg.Model
	}

	params := openai.ChatCompletionNewParams{
		Model:    shared.ChatModel(model),
		Messages: c.buildMessages(req),
	}
	if req.MaxTokens > 0 {
		params.MaxTokens = openai.Int(int64(req.MaxTokens))
	} else if c.cfg.MaxTokens > 0 {
		params.MaxTokens = openai.Int(int64(c.cfg.MaxTokens))
	}
	if req.Temperature > 0 {
		params.Temperature = openai.Float(float64(req.Temperature))
	} else if c.cfg.Temperature > 0 {
		params.Temperature = openai.Float(float64(c.cfg.Temperature))
	}
	if rf := c.buildResponseFormat(req.ResponseFormat); rf != nil {
		params.ResponseFormat = *rf
	}

	comp, err := c.client.Chat.Completions.New(ctx, params)
	if err != nil {
		return nil, fmt.Errorf("openai chat completion: %w", err)
	}
	if len(comp.Choices) == 0 {
		return nil, fmt.Errorf("openai response had no choices")
	}
	choice := comp.Choices[0]
	return &Response{
		Text:       choice.Message.Content,
		StopReason: choice.FinishReason,
		Usage: Usage{
			InputTokens:  int(comp.Usage.PromptTokens),
			OutputTokens: int(comp.Usage.CompletionTokens),
		},
	}, nil
}

func (c *OpenAIClient) buildMessages(req Request) []openai.ChatCompletionMessageParamUnion {
	msgs := make([]openai.ChatCompletionMessageParamUnion, 0, len(req.Messages)+1)
	if req.System != "" {
		msgs = append(msgs, openai.SystemMessage(req.System))
	}
	for _, m := range req.Messages {
		text := joinContentParts(m.Content)
		switch m.Role {
		case RoleSystem:
			msgs = append(msgs, openai.SystemMessage(text))
		case RoleAssistant:
			msgs = append(msgs, openai.AssistantMessage(text))
		default:
			msgs = append(msgs, openai.UserMessage(text))
		}
	}
	return msgs
}

func (c *OpenAIClient) buildResponseFormat(rf ResponseFormat) *openai.ChatCompletionNewParamsResponseFormatUnion {
	switch rf.Type {
	case ResponseFormatText:
		return &openai.ChatCompletionNewParamsResponseFormatUnion{
			OfText: &shared.ResponseFormatTextParam{},
		}
	case ResponseFormatJSONObject:
		return &openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONObject: &shared.ResponseFormatJSONObjectParam{},
		}
	case ResponseFormatJSONSchema:
		name := schemaName(rf.JSONSchema)
		param := shared.ResponseFormatJSONSchemaJSONSchemaParam{
			Name:   name,
			Strict: openai.Bool(false),
		}
		if rf.JSONSchema != nil {
			param.Schema = rf.JSONSchema
		}
		return &openai.ChatCompletionNewParamsResponseFormatUnion{
			OfJSONSchema: &shared.ResponseFormatJSONSchemaParam{
				JSONSchema: param,
			},
		}
	}
	return nil
}

// schemaName derives a valid structured-output name from a JSON schema's
// "title" field, falling back to "plan". The OpenAI API requires the name to
// match [a-zA-Z0-9_-] and be at most 64 chars.
func schemaName(schema map[string]any) string {
	if v, ok := schema["title"].(string); ok && v != "" {
		return sanitizeSchemaName(v)
	}
	return "plan"
}

func sanitizeSchemaName(s string) string {
	var b strings.Builder
	for _, r := range s {
		switch {
		case r >= 'a' && r <= 'z', r >= 'A' && r <= 'Z', r >= '0' && r <= '9', r == '_' || r == '-':
			b.WriteRune(r)
		default:
			b.WriteByte('_')
		}
	}
	out := b.String()
	if len(out) > 64 {
		out = out[:64]
	}
	if out == "" {
		return "plan"
	}
	return out
}

func joinContentParts(parts []ContentPart) string {
	var b strings.Builder
	for _, p := range parts {
		if p.Type == "text" || p.Type == "" {
			b.WriteString(p.Text)
		}
	}
	return b.String()
}
