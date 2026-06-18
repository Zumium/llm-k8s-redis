package llm

import (
	"context"
	"errors"
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
		"provider":  "openai",
		"baseUrl":   "https://api.openai.com/v1",
		"apiKey":    "sk-test",
		"model":     "gpt-4o",
		"maxTokens": "4096",
	}
}

func TestConfigMapSource_ReloadSuccess(t *testing.T) {
	scheme := newScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(makeConfigMap("llm-config", "redis-cluster-system", validConfigData())).
		Build()

	src := NewConfigMapSource(cl, types.NamespacedName{Name: "llm-config", Namespace: "redis-cluster-system"})
	if _, _, err := src.Get(); !errors.Is(err, ErrConfigNotLoaded) {
		t.Fatalf("expected ErrConfigNotLoaded before reload, got %v", err)
	}

	if err := src.Reload(context.Background()); err != nil {
		t.Fatalf("reload: %v", err)
	}

	cfg, c, err := src.Get()
	if err != nil {
		t.Fatalf("get: %v", err)
	}
	if cfg.Model != "gpt-4o" {
		t.Errorf("model = %q", cfg.Model)
	}
	if c == nil {
		t.Error("client should not be nil after reload")
	}
}

func TestConfigMapSource_ReloadNotFound(t *testing.T) {
	scheme := newScheme(t)
	cl := fake.NewClientBuilder().WithScheme(scheme).Build()

	src := NewConfigMapSource(cl, types.NamespacedName{Name: "missing", Namespace: "ns"})
	if err := src.Reload(context.Background()); err == nil {
		t.Fatal("expected error for missing configmap")
	}
	if _, _, err := src.Get(); !errors.Is(err, ErrConfigNotLoaded) {
		t.Errorf("expected ErrConfigNotLoaded after failed reload, got %v", err)
	}
}

func TestConfigMapSource_ReloadInvalidConfig(t *testing.T) {
	scheme := newScheme(t)
	cl := fake.NewClientBuilder().
		WithScheme(scheme).
		WithObjects(makeConfigMap("llm-config", "ns", map[string]string{"baseUrl": "x", "apiKey": "k"})).
		Build()

	src := NewConfigMapSource(cl, types.NamespacedName{Name: "llm-config", Namespace: "ns"})
	if err := src.Reload(context.Background()); err == nil {
		t.Fatal("expected error for invalid config (missing model)")
	}
}

func TestConfigMapSource_ReloadPicksUpChanges(t *testing.T) {
	scheme := newScheme(t)
	cm := makeConfigMap("llm-config", "ns", validConfigData())
	cl := fake.NewClientBuilder().WithScheme(scheme).WithObjects(cm).Build()

	src := NewConfigMapSource(cl, types.NamespacedName{Name: "llm-config", Namespace: "ns"})
	if err := src.Reload(context.Background()); err != nil {
		t.Fatalf("reload 1: %v", err)
	}
	cfg1, _, _ := src.Get()
	if cfg1.Model != "gpt-4o" {
		t.Fatalf("model1 = %q", cfg1.Model)
	}

	cm.Data["model"] = "gpt-4o-mini"
	if err := cl.Update(context.Background(), cm); err != nil {
		t.Fatalf("update configmap: %v", err)
	}
	if err := src.Reload(context.Background()); err != nil {
		t.Fatalf("reload 2: %v", err)
	}
	cfg2, _, _ := src.Get()
	if cfg2.Model != "gpt-4o-mini" {
		t.Errorf("model2 = %q, want gpt-4o-mini after update", cfg2.Model)
	}
}
