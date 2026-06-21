package llm

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

func LoadConfigMap(ctx context.Context, reader client.Reader, key types.NamespacedName) (Config, Client, error) {
	var cm corev1.ConfigMap
	if err := reader.Get(ctx, key, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return Config{}, nil, fmt.Errorf("llm configmap %s not found: %w", key, err)
		}
		return Config{}, nil, fmt.Errorf("get llm configmap: %w", err)
	}
	cfg, err := ParseConfig(cm.Data)
	if err != nil {
		return Config{}, nil, fmt.Errorf("parse llm configmap %s: %w", key, err)
	}
	c, err := NewOpenAIClient(cfg)
	if err != nil {
		return Config{}, nil, fmt.Errorf("build llm client from configmap %s: %w", key, err)
	}
	log.FromContext(ctx).Info("llm config loaded", "configmap", key, "provider", cfg.Provider, "model", cfg.Model)
	return cfg, c, nil
}
