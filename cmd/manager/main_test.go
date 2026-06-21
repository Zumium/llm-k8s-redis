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
