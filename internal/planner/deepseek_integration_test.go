package planner

import (
	"context"
	"encoding/json"
	"os"
	"testing"

	"github.com/example/llm-k8s-redis/internal/llm"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// TestDeepSeekIntegration exercises the full chain OpenAIClient -> LLMPlanner ->
// Validator against the real DeepSeek API. It is skipped unless DEEPSEEK_API_KEY
// is set, so it does not run during normal `go test`.
//
// Run:
//
//	DEEPSEEK_API_KEY=sk-... go test ./internal/planner/ -run TestDeepSeekIntegration -v
func TestDeepSeekIntegration(t *testing.T) {
	apiKey := os.Getenv("DEEPSEEK_API_KEY")
	if apiKey == "" {
		t.Skip("DEEPSEEK_API_KEY not set; skipping live integration test")
	}

	cfg := llm.Config{
		Provider:  llm.ProviderOpenAI,
		BaseURL:   "https://api.deepseek.com/v1",
		APIKey:    apiKey,
		Model:     "deepseek-v4-flash",
		MaxTokens: 8192,
	}
	client, err := llm.NewOpenAIClient(cfg)
	if err != nil {
		t.Fatalf("new openai client: %v", err)
	}

	spec := plan.ClusterSpec{
		Name:             "example",
		Generation:       1,
		Shards:           2,
		ReplicasPerShard: 1,
		Image:            "redis:7.2",
		MemorySize:       "2Gi",
	}
	p := NewLLMPlanner(client, cfg.Model)

	got, err := p.Plan(context.Background(), Request{
		Spec: spec,
	})
	if err != nil {
		t.Fatalf("planner error: %v", err)
	}

	pretty, _ := json.MarshalIndent(got, "", "  ")
	t.Logf("generated plan:\n%s", string(pretty))

	if err := plan.NewValidator().Validate(got, spec); err != nil {
		t.Fatalf("validator rejected the LLM-generated plan: %v", err)
	}
	t.Logf("plan passed deterministic validation: steps=%d", len(got.Steps))
}
