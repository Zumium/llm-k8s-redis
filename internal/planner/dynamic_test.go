package planner

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/llm"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// stubSource is a test PlannerSource that returns a fixed config/client or error.
type stubSource struct {
	cfg    llm.Config
	client llm.Client
	err    error
}

func (s *stubSource) Get() (llm.Config, llm.Client, error) {
	return s.cfg, s.client, s.err
}

func TestDynamicPlanner_UsesSource(t *testing.T) {
	fakeClient := &fakeLLMClient{resp: &llm.Response{Text: validCreatePlanJSON()}}
	src := &stubSource{
		cfg:    llm.Config{Model: "gpt-4o", MaxTokens: 2048, Temperature: 0.1, Provider: llm.ProviderOpenAI, BaseURL: "u", APIKey: "k"},
		client: fakeClient,
	}
	dp := NewDynamicPlanner(src)

	got, err := dp.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.TargetGeneration != sampleSpec().Generation {
		t.Errorf("targetGeneration = %d", got.TargetGeneration)
	}
	if fakeClient.lastReq.Model != "gpt-4o" {
		t.Errorf("model passed to client = %q, want gpt-4o", fakeClient.lastReq.Model)
	}
}

func TestDynamicPlanner_NotLoaded(t *testing.T) {
	src := &stubSource{err: llm.ErrConfigNotLoaded}
	dp := NewDynamicPlanner(src)

	_, err := dp.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err == nil {
		t.Fatal("expected error when source not loaded")
	}
	if !errors.Is(err, llm.ErrConfigNotLoaded) {
		t.Errorf("expected ErrConfigNotLoaded, got %v", err)
	}
}

func TestDynamicPlanner_NilClient(t *testing.T) {
	src := &stubSource{cfg: llm.Config{Model: "m"}, client: nil}
	dp := NewDynamicPlanner(src)

	_, err := dp.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err == nil {
		t.Fatal("expected error for nil client")
	}
}

func TestDynamicPlanner_NilSource(t *testing.T) {
	dp := &DynamicPlanner{}
	_, err := dp.Plan(context.Background(), Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err == nil {
		t.Fatal("expected error for nil source")
	}
}

func TestDynamicPlanner_HotReload(t *testing.T) {
	client1 := &fakeLLMClient{resp: &llm.Response{Text: validCreatePlanJSON()}}
	src := &stubSource{
		cfg:    llm.Config{Model: "gpt-4o"},
		client: client1,
	}
	dp := NewDynamicPlanner(src)

	if _, err := dp.Plan(context.Background(), Request{Cluster: sampleCluster(), Spec: sampleSpec(), Operation: plan.OpCreate}); err != nil {
		t.Fatalf("plan 1: %v", err)
	}
	if client1.lastReq.Model != "gpt-4o" {
		t.Errorf("plan 1 model = %q", client1.lastReq.Model)
	}

	client2 := &fakeLLMClient{resp: &llm.Response{Text: validCreatePlanJSON()}}
	src.cfg = llm.Config{Model: "gpt-4o-mini"}
	src.client = client2

	if _, err := dp.Plan(context.Background(), Request{Cluster: sampleCluster(), Spec: sampleSpec(), Operation: plan.OpCreate}); err != nil {
		t.Fatalf("plan 2: %v", err)
	}
	if client2.lastReq.Model != "gpt-4o-mini" {
		t.Errorf("plan 2 model = %q, want gpt-4o-mini (hot reload)", client2.lastReq.Model)
	}
}

func TestPlanWithClient_FixesFields(t *testing.T) {
	raw := validCreatePlanJSON()
	var m map[string]any
	_ = json.Unmarshal([]byte(raw), &m)
	m["dslVersion"] = "wrong"
	m["targetGeneration"] = 999
	fixed, _ := json.Marshal(m)

	fc := &fakeLLMClient{resp: &llm.Response{Text: string(fixed)}}
	got, err := planWithClient(context.Background(), fc, "m", 0, 0, Request{
		Cluster:   sampleCluster(),
		Spec:      sampleSpec(),
		Operation: plan.OpCreate,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got.DSLVersion != plan.DSLVersion {
		t.Errorf("dslVersion = %q", got.DSLVersion)
	}
	if got.TargetGeneration != sampleSpec().Generation {
		t.Errorf("targetGeneration = %d", got.TargetGeneration)
	}
}

// reuse sample helpers from llm_planner_test.go (same package).
var _ = v1alpha1.RedisCluster{}
