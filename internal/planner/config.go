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

	if err := config.Validate(); err != nil {
		return Config{}, err
	}
	return config, nil
}

func validReasoningEffort(v string) bool {
	switch v {
	case "", "high", "max", "low", "medium", "xhigh":
		return true
	default:
		return false
	}
}
