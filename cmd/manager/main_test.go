package main

import (
	"context"
	"testing"

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
			"provider":        "openai",
			"baseUrl":         "https://api.openai.com/v1",
			"apiKey":          "sk-test",
			"model":           "deepseek-v4-flash",
			"maxTokens":       "2048",
			"temperature":     "0.1",
			"reasoningEffort": "max",
		},
	}
	cl := fake.NewClientBuilder().WithScheme(s).WithObjects(cm).Build()

	p, cfg, err := newLLMPlannerFromConfigMap(context.Background(), cl, types.NamespacedName{Name: "llm-config", Namespace: "ns"})
	if err != nil {
		t.Fatalf("new planner: %v", err)
	}
	if p.Model != "deepseek-v4-flash" || cfg.Model != p.Model {
		t.Errorf("model cfg=%q planner=%q", cfg.Model, p.Model)
	}
	if p.MaxTokens != 2048 {
		t.Errorf("maxTokens = %d", p.MaxTokens)
	}
	if p.Temperature != 0.1 {
		t.Errorf("temperature = %v", p.Temperature)
	}
	if p.ReasoningEffort != "max" {
		t.Errorf("reasoningEffort = %q", p.ReasoningEffort)
	}
	if p.Client == nil {
		t.Fatal("client is nil")
	}
}
