package llm

import (
	"testing"
)

func TestParseConfig_Valid(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"provider":        "openai",
		"baseUrl":         "https://api.openai.com/v1",
		"apiKey":          "sk-test",
		"model":           "gpt-4o",
		"maxTokens":       "4096",
		"temperature":     "0.2",
		"reasoningEffort": "max",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Provider != ProviderOpenAI {
		t.Errorf("provider = %q, want openai", cfg.Provider)
	}
	if cfg.BaseURL != "https://api.openai.com/v1" {
		t.Errorf("baseUrl = %q", cfg.BaseURL)
	}
	if cfg.Model != "gpt-4o" {
		t.Errorf("model = %q", cfg.Model)
	}
	if cfg.MaxTokens != 4096 {
		t.Errorf("maxTokens = %d", cfg.MaxTokens)
	}
	if cfg.Temperature != 0.2 {
		t.Errorf("temperature = %v", cfg.Temperature)
	}
	if cfg.ReasoningEffort != "max" {
		t.Errorf("reasoningEffort = %q", cfg.ReasoningEffort)
	}
}

func TestParseConfig_UsesDefaults(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"baseUrl": "https://api.openai.com/v1",
		"apiKey":  "sk-test",
		"model":   "gpt-4o",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Provider != ProviderOpenAI {
		t.Errorf("default provider = %q, want openai", cfg.Provider)
	}
	if cfg.MaxTokens != 8192 {
		t.Errorf("default maxTokens = %d, want 8192", cfg.MaxTokens)
	}
	if cfg.ReasoningEffort != "" {
		t.Errorf("default reasoningEffort = %q, want empty", cfg.ReasoningEffort)
	}
}

func TestParseConfig_MissingFields(t *testing.T) {
	cases := []struct {
		name string
		data map[string]string
	}{
		{"missing baseUrl", map[string]string{"apiKey": "k", "model": "m"}},
		{"missing apiKey", map[string]string{"baseUrl": "u", "model": "m"}},
		{"missing model", map[string]string{"baseUrl": "u", "apiKey": "k"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := ParseConfig(c.data); err == nil {
				t.Fatal("expected error for missing field")
			}
		})
	}
}

func TestParseConfig_ReasoningEffortValues(t *testing.T) {
	for _, effort := range []string{"high", "max", "low", "medium", "xhigh", ""} {
		t.Run(effort, func(t *testing.T) {
			_, err := ParseConfig(map[string]string{
				"baseUrl":         "u",
				"apiKey":          "k",
				"model":           "m",
				"reasoningEffort": effort,
			})
			if err != nil {
				t.Fatalf("unexpected error for %q: %v", effort, err)
			}
		})
	}
}

func TestParseConfig_BadReasoningEffort(t *testing.T) {
	_, err := ParseConfig(map[string]string{
		"baseUrl":         "u",
		"apiKey":          "k",
		"model":           "m",
		"reasoningEffort": "minimal",
	})
	if err == nil {
		t.Fatal("expected error for invalid reasoningEffort")
	}
}

func TestParseConfig_BadNumbers(t *testing.T) {
	cases := []struct {
		name string
		data map[string]string
	}{
		{"bad maxTokens", map[string]string{"baseUrl": "u", "apiKey": "k", "model": "m", "maxTokens": "abc"}},
		{"bad temperature", map[string]string{"baseUrl": "u", "apiKey": "k", "model": "m", "temperature": "xx"}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if _, err := ParseConfig(c.data); err == nil {
				t.Fatal("expected error for bad number")
			}
		})
	}
}

func TestParseConfig_TLSInsecureSkipVerify(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"baseUrl":               "u",
		"apiKey":                "k",
		"model":                 "m",
		"tlsInsecureSkipVerify": "true",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if !cfg.TLSInsecureSkipVerify {
		t.Error("expected TLSInsecureSkipVerify=true")
	}
}

func TestParseConfig_TLSInsecureSkipVerifyDefaultFalse(t *testing.T) {
	cfg, err := ParseConfig(map[string]string{
		"baseUrl": "u",
		"apiKey":  "k",
		"model":   "m",
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.TLSInsecureSkipVerify {
		t.Error("expected TLSInsecureSkipVerify=false by default")
	}
}

func TestParseConfig_UnknownProvider(t *testing.T) {
	_, err := ParseConfig(map[string]string{
		"provider": "acme",
		"baseUrl":  "u",
		"apiKey":   "k",
		"model":    "m",
	})
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
}
