package planner

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

type Config struct {
	BaseURL               string
	APIKey                string
	Model                 string
	MaxTokens             int
	Temperature           float32
	ReasoningEffort       string
	PlanValidationRetries int
	EmbeddingModel        string
	EmbeddingBaseURL      string
	EmbeddingAPIKey       string
}

func DefaultConfig() Config {
	return Config{
		MaxTokens:             8192,
		PlanValidationRetries: 1,
	}
}

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
	if !validReasoningEffort(c.ReasoningEffort) {
		return fmt.Errorf("llm config: invalid reasoningEffort %q", c.ReasoningEffort)
	}
	if c.PlanValidationRetries < 0 || c.PlanValidationRetries > 10 {
		return fmt.Errorf("llm config: planValidationRetries must be between 0 and 10")
	}
	if err := validateEmbeddingConfig(c); err != nil {
		return err
	}
	return nil
}

func ParseConfig(data map[string]string) (Config, error) {
	config := DefaultConfig()
	if value, ok := data["baseUrl"]; ok {
		config.BaseURL = strings.TrimSpace(value)
	}
	if value, ok := data["apiKey"]; ok {
		config.APIKey = strings.TrimSpace(value)
	}
	if value, ok := data["model"]; ok {
		config.Model = strings.TrimSpace(value)
	}

	if value, ok := data["maxTokens"]; ok && value != "" {
		maxTokens, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return Config{}, fmt.Errorf("llm config: invalid maxTokens %q: %w", value, err)
		}
		config.MaxTokens = maxTokens
	}
	if value, ok := data["temperature"]; ok && value != "" {
		temperature, err := strconv.ParseFloat(strings.TrimSpace(value), 32)
		if err != nil {
			return Config{}, fmt.Errorf("llm config: invalid temperature %q: %w", value, err)
		}
		config.Temperature = float32(temperature)
	}
	if value, ok := data["reasoningEffort"]; ok {
		config.ReasoningEffort = strings.TrimSpace(value)
	}
	if value, ok := data["planValidationRetries"]; ok && value != "" {
		retries, err := strconv.Atoi(strings.TrimSpace(value))
		if err != nil {
			return Config{}, fmt.Errorf("llm config: invalid planValidationRetries %q: %w", value, err)
		}
		config.PlanValidationRetries = retries
	}

	if value, ok := data["embeddingModel"]; ok {
		config.EmbeddingModel = strings.TrimSpace(value)
	}
	if value, ok := data["embeddingBaseUrl"]; ok {
		config.EmbeddingBaseURL = strings.TrimSpace(value)
	}
	if value, ok := data["embeddingApiKey"]; ok {
		config.EmbeddingAPIKey = strings.TrimSpace(value)
	}

	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func (c Config) HasEmbeddingConfig() bool {
	return c.EmbeddingModel != "" && c.EmbeddingBaseURL != "" && c.EmbeddingAPIKey != ""
}

func validateEmbeddingConfig(c Config) error {
	hasModel := c.EmbeddingModel != ""
	hasBaseURL := c.EmbeddingBaseURL != ""
	hasAPIKey := c.EmbeddingAPIKey != ""
	switch {
	case !hasModel && !hasBaseURL && !hasAPIKey:
		return nil
	case !hasModel:
		return errors.New("llm config: embeddingModel is required when other embedding fields are set")
	case !hasBaseURL:
		return errors.New("llm config: embeddingBaseUrl is required when other embedding fields are set")
	case !hasAPIKey:
		return errors.New("llm config: embeddingApiKey is required when other embedding fields are set")
	default:
		return nil
	}
}

func validReasoningEffort(v string) bool {
	switch v {
	case "", "high", "max", "low", "medium", "xhigh":
		return true
	default:
		return false
	}
}
