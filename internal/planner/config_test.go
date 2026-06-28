package planner

import "testing"

func TestParseConfig_Valid(t *testing.T) {
	config, err := ParseConfig(map[string]string{
		"provider":              "ignored",
		"baseUrl":               "https://api.openai.com/v1",
		"apiKey":                "sk-test",
		"model":                 "gpt-4o",
		"maxTokens":             "4096",
		"temperature":           "0.2",
		"reasoningEffort":       "max",
		"planValidationRetries": "3",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.BaseURL != "https://api.openai.com/v1" {
		t.Errorf("baseUrl = %q", config.BaseURL)
	}
	if config.Model != "gpt-4o" {
		t.Errorf("model = %q", config.Model)
	}
	if config.MaxTokens != 4096 {
		t.Errorf("maxTokens = %d", config.MaxTokens)
	}
	if config.Temperature != 0.2 {
		t.Errorf("temperature = %v", config.Temperature)
	}
	if config.ReasoningEffort != "max" {
		t.Errorf("reasoningEffort = %q", config.ReasoningEffort)
	}
	if config.PlanValidationRetries != 3 {
		t.Errorf("planValidationRetries = %d", config.PlanValidationRetries)
	}
}

func TestParseConfig_UsesDefaults(t *testing.T) {
	config, err := ParseConfig(map[string]string{
		"baseUrl": "https://api.openai.com/v1",
		"apiKey":  "sk-test",
		"model":   "gpt-4o",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.MaxTokens != 8192 {
		t.Errorf("default maxTokens = %d, want 8192", config.MaxTokens)
	}
	if config.PlanValidationRetries != 1 {
		t.Errorf("default planValidationRetries = %d, want 1", config.PlanValidationRetries)
	}
}

func TestParseConfig_MissingFields(t *testing.T) {
	for _, data := range []map[string]string{
		{"apiKey": "k", "model": "m"},
		{"baseUrl": "u", "model": "m"},
		{"baseUrl": "u", "apiKey": "k"},
	} {
		if _, err := ParseConfig(data); err == nil {
			t.Fatalf("expected error for %#v", data)
		}
	}
}

func TestParseConfig_ReasoningEffortValues(t *testing.T) {
	for _, effort := range []string{"high", "max", "low", "medium", "xhigh", ""} {
		_, err := ParseConfig(map[string]string{
			"baseUrl":         "u",
			"apiKey":          "k",
			"model":           "m",
			"reasoningEffort": effort,
		})
		if err != nil {
			t.Fatalf("unexpected error for %q: %v", effort, err)
		}
	}
}

func TestParseConfig_BadValues(t *testing.T) {
	for _, data := range []map[string]string{
		{"baseUrl": "u", "apiKey": "k", "model": "m", "reasoningEffort": "minimal"},
		{"baseUrl": "u", "apiKey": "k", "model": "m", "maxTokens": "abc"},
		{"baseUrl": "u", "apiKey": "k", "model": "m", "temperature": "xx"},
		{"baseUrl": "u", "apiKey": "k", "model": "m", "planValidationRetries": "xx"},
		{"baseUrl": "u", "apiKey": "k", "model": "m", "planValidationRetries": "-1"},
		{"baseUrl": "u", "apiKey": "k", "model": "m", "planValidationRetries": "11"},
	} {
		if _, err := ParseConfig(data); err == nil {
			t.Fatalf("expected error for %#v", data)
		}
	}
}

func TestParseConfig_EmbeddingAllSet(t *testing.T) {
	config, err := ParseConfig(map[string]string{
		"baseUrl":          "u",
		"apiKey":           "k",
		"model":            "m",
		"embeddingModel":   "text-embedding-3-small",
		"embeddingBaseUrl": "https://api.openai.com/v1",
		"embeddingApiKey":  "sk-emb",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !config.HasEmbeddingConfig() {
		t.Fatal("expected HasEmbeddingConfig to be true")
	}
}

func TestParseConfig_EmbeddingNone(t *testing.T) {
	config, err := ParseConfig(map[string]string{
		"baseUrl": "u",
		"apiKey":  "k",
		"model":   "m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if config.HasEmbeddingConfig() {
		t.Fatal("expected HasEmbeddingConfig to be false")
	}
}

func TestParseConfig_EmbeddingPartial(t *testing.T) {
	for _, data := range []map[string]string{
		{"baseUrl": "u", "apiKey": "k", "model": "m", "embeddingModel": "text-embedding-3-small"},
		{"baseUrl": "u", "apiKey": "k", "model": "m", "embeddingModel": "text-embedding-3-small", "embeddingBaseUrl": "https://api.openai.com/v1"},
		{"baseUrl": "u", "apiKey": "k", "model": "m", "embeddingBaseUrl": "https://api.openai.com/v1"},
	} {
		if _, err := ParseConfig(data); err == nil {
			t.Fatalf("expected error for %#v", data)
		}
	}
}
