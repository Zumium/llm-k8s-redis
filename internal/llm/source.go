package llm

import (
	"context"
	"errors"
	"fmt"
	"sync/atomic"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"k8s.io/apimachinery/pkg/types"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"
)

// ErrConfigNotLoaded is returned by ConfigMapSource.Get before the first
// successful load.
var ErrConfigNotLoaded = errors.New("llm config not loaded yet")

// PollInterval is how often the source re-reads the ConfigMap to pick up
// changes (e.g. rotated API keys). It is deliberately coarse: ConfigMap-driven
// LLM config changes rarely.
const PollInterval = 15 * time.Second

// loaded holds the immutable snapshot produced by a successful reload.
type loaded struct {
	cfg    Config
	client Client
}

// ConfigMapSource reads LLM config from a ConfigMap and exposes the current
// Config and Client. It is safe for concurrent use and hot-reloads by polling
// the ConfigMap.
//
// It implements manager.Runnable: Start performs an initial load, then polls
// on PollInterval until the context is cancelled. A failed initial load does
// NOT crash the manager — the controller surfaces "planner not configured"
// until a reload succeeds.
type ConfigMapSource struct {
	client client.Client
	key    types.NamespacedName

	current atomic.Pointer[loaded]
}

// NewConfigMapSource returns a source backed by the given ConfigMap key.
func NewConfigMapSource(cl client.Client, key types.NamespacedName) *ConfigMapSource {
	return &ConfigMapSource{
		client: cl,
		key:    key,
	}
}

// Get returns the current Config and Client. It returns ErrConfigNotLoaded
// before the first successful reload.
func (s *ConfigMapSource) Get() (Config, Client, error) {
	l := s.current.Load()
	if l == nil {
		return Config{}, nil, ErrConfigNotLoaded
	}
	return l.cfg, l.client, nil
}

// Reload forces an immediate re-read of the ConfigMap and rebuilds the client.
func (s *ConfigMapSource) Reload(ctx context.Context) error {
	var cm corev1.ConfigMap
	if err := s.client.Get(ctx, s.key, &cm); err != nil {
		if apierrors.IsNotFound(err) {
			return fmt.Errorf("llm configmap %s not found: %w", s.key, err)
		}
		return fmt.Errorf("get llm configmap: %w", err)
	}
	cfg, err := ParseConfig(cm.Data)
	if err != nil {
		return fmt.Errorf("parse llm configmap %s: %w", s.key, err)
	}
	c, err := NewOpenAIClient(cfg)
	if err != nil {
		return fmt.Errorf("build llm client from configmap %s: %w", s.key, err)
	}
	prev := s.current.Load()
	s.current.Store(&loaded{cfg: cfg, client: c})
	if prev == nil {
		log.FromContext(ctx).Info("llm config loaded", "configmap", s.key, "provider", cfg.Provider, "model", cfg.Model)
	} else if prev.cfg != cfg {
		log.FromContext(ctx).Info("llm config reloaded", "configmap", s.key, "provider", cfg.Provider, "model", cfg.Model)
	}
	return nil
}

// Start implements manager.Runnable.
func (s *ConfigMapSource) Start(ctx context.Context) error {
	logger := log.FromContext(ctx).WithValues("configmap", s.key)

	if err := s.Reload(ctx); err != nil {
		logger.Error(err, "initial llm config load failed; will retry")
	}

	ticker := time.NewTicker(PollInterval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			if err := s.Reload(ctx); err != nil {
				logger.Error(err, "reload llm config failed")
			}
		}
	}
}
