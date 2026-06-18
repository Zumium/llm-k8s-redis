package llm

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// Provider identifies the LLM backend. The HTTP client is OpenAI-compatible;
// "anthropic" routes to Anthropic's OpenAI-compatible endpoint semantics where
// relevant, but both use the same chat-completions request shape today.
type Provider string

const (
	ProviderOpenAI    Provider = "openai"
	ProviderAnthropic Provider = "anthropic"
)

// Config holds the LLM connection settings. It is loaded from a ConfigMap and
// used to build an llm.Client.
type Config struct {
	Provider    Provider
	BaseURL     string
	APIKey      string
	Model       string
	MaxTokens   int
	Temperature float32
}

// DefaultConfig returns a Config with sensible defaults for unset numeric fields.
func DefaultConfig() Config {
	return Config{
		Provider:    ProviderOpenAI,
		MaxTokens:   8192,
		Temperature: 0,
	}
}

// Validate ensures the config has the minimum fields required to call the LLM.
func (c Config) Validate() error {
	if c.BaseURL == "" {
		return errors.New("llm config: baseUrl is required")
	}
	if c.APIKey == "" {
		return errors.New("llm config: apiKey is required")
	}
	if c.Model == "" {
		return errors.New("llm config: model is required")
	}
	switch c.Provider {
	case ProviderOpenAI, ProviderAnthropic:
	default:
		return fmt.Errorf("llm config: unknown provider %q", c.Provider)
	}
	return nil
}

// ParseConfig builds a Config from ConfigMap string data. Recognized keys:
// provider, baseUrl, apiKey, model, maxTokens, temperature. Unknown keys are
// ignored so the ConfigMap can carry extra metadata.
func ParseConfig(data map[string]string) (Config, error) {
	cfg := DefaultConfig()
	if v, ok := data["provider"]; ok && v != "" {
		cfg.Provider = Provider(v)
	}
	if v, ok := data["baseUrl"]; ok {
		cfg.BaseURL = strings.TrimSpace(v)
	}
	if v, ok := data["apiKey"]; ok {
		cfg.APIKey = strings.TrimSpace(v)
	}
	if v, ok := data["model"]; ok {
		cfg.Model = strings.TrimSpace(v)
	}
	if v, ok := data["maxTokens"]; ok && v != "" {
		n, err := strconv.Atoi(strings.TrimSpace(v))
		if err != nil {
			return Config{}, fmt.Errorf("llm config: invalid maxTokens %q: %w", v, err)
		}
		cfg.MaxTokens = n
	}
	if v, ok := data["temperature"]; ok && v != "" {
		f, err := strconv.ParseFloat(strings.TrimSpace(v), 32)
		if err != nil {
			return Config{}, fmt.Errorf("llm config: invalid temperature %q: %w", v, err)
		}
		cfg.Temperature = float32(f)
	}
	if err := cfg.Validate(); err != nil {
		return Config{}, err
	}
	return cfg, nil
}
