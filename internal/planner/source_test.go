package planner

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

func newScheme(t *testing.T) *runtime.Scheme {
	t.Helper()
	s := runtime.NewScheme()
	if err := clientgoscheme.AddToScheme(s); err != nil {
		t.Fatalf("add clientgo scheme: %v", err)
	}
	return s
}

func makeConfigMap(name, ns string, data map[string]string) *corev1.ConfigMap {
	return &corev1.ConfigMap{
		ObjectMeta: metav1.ObjectMeta{Name: name, Namespace: ns},
		Data:       data,
	}
}

func validConfigData() map[string]string {
	return map[string]string{
		"provider":              "openai",
		"baseUrl":               "https://api.openai.com/v1",
		"apiKey":                "sk-test",
		"model":                 "gpt-4o",
		"maxTokens":             "4096",
		"temperature":           "0.2",
		"reasoningEffort":       "max",
		"planValidationRetries": "2",
	}
}

func TestLoadConfigMap(t *testing.T) {
	scheme := newScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(makeConfigMap("llm-config", "redis-cluster-system", validConfigData())).
		Build()

	config, llmClient, err := LoadConfigMap(context.Background(), cl, types.NamespacedName{Name: "llm-config", Namespace: "redis-cluster-system"})
	if err != nil {
		t.Fatalf("load: %v", err)
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
	if config.PlanValidationRetries != 2 {
		t.Errorf("planValidationRetries = %d", config.PlanValidationRetries)
	}
	if llmClient == nil {
		t.Error("client should not be nil")
	}
}

func TestLoadConfigMap_NotFound(t *testing.T) {
	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	if _, _, err := LoadConfigMap(context.Background(), cl, types.NamespacedName{Name: "missing", Namespace: "ns"}); err == nil {
		t.Fatal("expected error for missing configmap")
	}
}

func TestLoadConfigMap_InvalidConfig(t *testing.T) {
	scheme := newScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(makeConfigMap("llm-config", "ns", map[string]string{"baseUrl": "x", "apiKey": "k"})).
		Build()

	if _, _, err := LoadConfigMap(context.Background(), cl, types.NamespacedName{Name: "llm-config", Namespace: "ns"}); err == nil {
		t.Fatal("expected error for invalid config")
	}
}
