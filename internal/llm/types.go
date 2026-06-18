// Package llm defines a provider-agnostic LLM client interface.
//
// The interface is intentionally minimal: it captures only what the planner
// needs to generate a Plan. Concrete adapters (OpenAI, Anthropic, etc.) live
// in separate packages and translate between this interface and their
// respective SDKs.
package llm

import "context"

// Client is the provider-agnostic LLM interface consumed by the planner.
//
// Implementations must be safe for concurrent use. A single Client instance is
// expected to be reused across all planner calls.
type Client interface {
	// Complete sends a chat-completion request and returns the generated
	// text. The caller (planner) is responsible for parsing the text into
	// a domain object.
	Complete(ctx context.Context, req Request) (*Response, error)
}

// Request is a provider-agnostic completion request.
type Request struct {
	// Model is the provider-specific model identifier, e.g.
	// "gpt-4o" or "claude-sonnet-4-5-20250929".
	Model string

	// System is the system prompt that sets the assistant's role and rules.
	System string

	// Messages is the ordered conversation history. The planner typically
	// sends a single user message containing the planning task.
	Messages []Message

	// ResponseFormat controls how the provider shapes the output. When set
	// to a JSON type, implementations should request structured JSON
	// output from the provider.
	ResponseFormat ResponseFormat

	// MaxTokens caps the output length. Zero means provider default.
	MaxTokens int

	// Temperature controls randomness. Zero means provider default.
	Temperature float32

	// ReasoningEffort controls reasoning depth for compatible providers.
	ReasoningEffort string

	// Metadata is an opaque bag for provider-specific extras (e.g. request
	// IDs, custom headers). The core interface does not interpret it.
	Metadata map[string]string
}

// Message is a single chat message.
type Message struct {
	Role    Role
	Content []ContentPart
}

// Role identifies who produced a message.
type Role string

const (
	RoleSystem    Role = "system"
	RoleUser      Role = "user"
	RoleAssistant Role = "assistant"
)

// ContentPart is one part of a message's content. The first version is
// text-only; image/tool parts can be added later without breaking existing
// callers that only read Text.
type ContentPart struct {
	// Type is the content part type, e.g. "text".
	Type string
	// Text is the text content when Type == "text".
	Text string
}

// ResponseFormat tells the provider how to shape the model's output.
type ResponseFormat struct {
	Type ResponseFormatType
	// JSONSchema is an optional JSON Schema describing the expected output.
	// Providers that support structured output should use it; others may
	// ignore it and rely on the prompt alone.
	JSONSchema map[string]any
}

// ResponseFormatType selects the output shaping mode.
type ResponseFormatType string

const (
	// ResponseFormatText lets the model produce free-form text.
	ResponseFormatText ResponseFormatType = "text"
	// ResponseFormatJSONObject asks the model to produce a single JSON object.
	ResponseFormatJSONObject ResponseFormatType = "json_object"
	// ResponseFormatJSONSchema asks the model to produce JSON conforming to
	// ResponseFormat.JSONSchema.
	ResponseFormatJSONSchema ResponseFormatType = "json_schema"
)

// Response is the result of a completion call.
type Response struct {
	// Text is the raw generated text. When ResponseFormat is a JSON type,
	// this should be valid JSON.
	Text string
	// StopReason is the provider-specific reason generation stopped
	// (e.g. "stop", "length", "tool_calls").
	StopReason string
	// Usage reports token consumption.
	Usage Usage
}

// Usage summarizes token consumption for billing and rate-limiting.
type Usage struct {
	InputTokens  int
	OutputTokens int
}
