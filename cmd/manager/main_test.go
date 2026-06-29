package main

import (
	"context"
	"errors"
	"testing"

	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	"sigs.k8s.io/controller-runtime/pkg/client/fake"
)

func TestNewLLMPlannerFromConfigMap(t *testing.T) {
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add scheme: %v", err)
	}
	cm := &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: "llm-config", Namespace: "ns"},
		Data: map[string]string{
			"provider":              "openai",
			"baseUrl":               "https://api.openai.com/v1",
			"apiKey":                "sk-test",
			"model":                 "deepseek-v4-flash",
			"maxTokens":             "2048",
			"temperature":           "0.1",
			"reasoningEffort":       "max",
			"planValidationRetries": "4",
		},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(cm).Build()

	p, config, err := newLLMPlannerFromConfigMap(context.Background(), cl, types.NamespacedName{Name: "llm-config", Namespace: "ns"})
	if err != nil {
		t.Fatalf("new planner: %v", err)
	}
	if config.Model != "deepseek-v4-flash" {
		t.Errorf("model = %q", config.Model)
	}
	if config.MaxTokens != 2048 {
		t.Errorf("maxTokens = %d", config.MaxTokens)
	}
	if config.Temperature != 0.1 {
		t.Errorf("temperature = %v", config.Temperature)
	}
	if config.ReasoningEffort != "max" {
		t.Errorf("reasoningEffort = %q", config.ReasoningEffort)
	}
	if config.PlanValidationRetries != 4 {
		t.Errorf("planValidationRetries = %d", config.PlanValidationRetries)
	}
	if p.LLMClient == nil {
		t.Fatal("client is nil")
	}
}

func TestPlannerForModeDisabledKeepsGoPlanner(t *testing.T) {
	p := plannerForMode(true, nil)
	spec := plan.ClusterSpec{Name: "example", Generation: 3, Shards: 2, ReplicasPerShard: 1, Image: "redis:7.2", MemorySize: "2Gi"}

	got, err := p.Plan(context.Background(), planner.Request{Spec: spec})
	if err != nil {
		t.Fatalf("deterministic create should work with llm disabled: %v", err)
	}
	if got.PlanID != "go-create-3" {
		t.Fatalf("planID = %q", got.PlanID)
	}

	_, err = p.Plan(context.Background(), planner.Request{Spec: plan.ClusterSpec{Name: "example", Generation: 3, Shards: 2, ReplicasPerShard: 0, Image: "redis:7.2", MemorySize: "2Gi"}})
	if !errors.Is(err, planner.ErrNotConfigured) {
		t.Fatalf("unsupported disabled plan err = %v", err)
	}
}
