package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"time"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	api "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/controller"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/planner"
	"github.com/Zumium/llm-k8s-redis/internal/rag"
	"github.com/Zumium/llm-k8s-redis/internal/redis"
)

var (
	scheme   = runtime.NewScheme()
	setupLog = ctrl.Log.WithName("setup")
)

func init() {
	utilruntime.Must(clientgoscheme.AddToScheme(scheme))
	utilruntime.Must(api.AddToScheme(scheme))
}

func main() {
	var (
		zapOpts                 zap.Options
		metricsAddr             string
		probeAddr               string
		enableLeaderElec        bool
		llmConfigMapName        string
		llmConfigMapNS          string
		disableLLMPlanner       bool
		topologyRefreshInterval time.Duration
		topologyStaleThreshold  time.Duration
	)
	zapOpts.Development = true
	zapOpts.BindFlags(flag.CommandLine)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Metrics server bind address.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Health probe bind address.")
	flag.BoolVar(&enableLeaderElec, "leader-elect", false, "Enable leader election.")
	flag.StringVar(&llmConfigMapName, "llm-configmap-name", "llm-config", "Name of the ConfigMap holding LLM connection config.")
	flag.StringVar(&llmConfigMapNS, "llm-configmap-namespace", "redis-cluster-system", "Namespace of the LLM ConfigMap.")
	flag.BoolVar(&disableLLMPlanner, "disable-llm-planner", false, "Use NoopPlanner instead of the ConfigMap-driven LLM planner (for testing).")
	flag.DurationVar(&topologyRefreshInterval, "topology-refresh-interval", 60*time.Second, "Interval between lazy topology refreshes for idle clusters.")
	flag.DurationVar(&topologyStaleThreshold, "topology-stale-threshold", 10*time.Second, "Minimum time between observeTopology calls to avoid Redis load spikes during Pod storms.")
	flag.Parse()
	ctrl.SetLogger(zap.New(zap.UseFlagOptions(&zapOpts)))

	mgr, err := ctrl.NewManager(ctrl.GetConfigOrDie(), ctrl.Options{
		Scheme:                 scheme,
		Metrics:                metricsserver.Options{BindAddress: metricsAddr},
		HealthProbeBindAddress: probeAddr,
		LeaderElection:         enableLeaderElec,
		LeaderElectionID:       "redis-cluster-controller.example.com",
	})
	if err != nil {
		setupLog.Error(err, "unable to start manager")
		os.Exit(1)
	}

	var p planner.Planner
	planValidationRetries := 0
	if !disableLLMPlanner {
		ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		llmPlanner, config, err := newLLMPlannerFromConfigMap(ctx, mgr.GetAPIReader(), types.NamespacedName{Name: llmConfigMapName, Namespace: llmConfigMapNS})
		if err != nil {
			setupLog.Error(err, "unable to load llm config")
			os.Exit(1)
		}
		p = plannerForMode(false, llmPlanner)
		planValidationRetries = config.PlanValidationRetries
		setupLog.Info("using go planner with llm fallback backed by configmap", "name", llmConfigMapName, "namespace", llmConfigMapNS, "model", config.Model)

		if config.HasEmbeddingConfig() {
			ret, err := newPlanRetrieverFromConfig(config)
			if err != nil {
				setupLog.Error(err, "unable to create rag plan retriever")
				os.Exit(1)
			}
			setupLog.Info("rag plan retriever enabled", "model", config.EmbeddingModel)
			_ = ret
		}
	} else {
		p = plannerForMode(true, nil)
		setupLog.Info("llm planner disabled; using go planner with NoopPlanner fallback")
	}

	executor := &controller.ActionExecutor{Client: mgr.GetClient(), Scheme: scheme, RedisFactory: redis.DefaultFactory}
	if err = (&controller.RedisClusterReconciler{
		Client:                  mgr.GetClient(),
		APIReader:               mgr.GetAPIReader(),
		Scheme:                  scheme,
		Planner:                 p,
		Driver:                  executor,
		ValidatePlan:            plan.NewValidator().Validate,
		Recorder:                mgr.GetEventRecorder("rediscluster-controller"),
		TopologyRefreshInterval: topologyRefreshInterval,
		TopologyStaleThreshold:  topologyStaleThreshold,
		PlanValidationRetries:   planValidationRetries,
	}).SetupWithManager(mgr); err != nil {
		setupLog.Error(err, "unable to create controller", "controller", "RedisCluster")
		os.Exit(1)
	}

	if err := mgr.AddHealthzCheck("healthz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up health check")
		os.Exit(1)
	}
	if err := mgr.AddReadyzCheck("readyz", healthz.Ping); err != nil {
		setupLog.Error(err, "unable to set up ready check")
		os.Exit(1)
	}

	setupLog.Info("starting manager")
	if err := mgr.Start(ctrl.SetupSignalHandler()); err != nil {
		setupLog.Error(err, "problem running manager")
		os.Exit(1)
	}
}

func plannerForMode(disableLLM bool, llmPlanner planner.Planner) planner.Planner {
	if disableLLM {
		return planner.NewGoPlanner(planner.NoopPlanner{})
	}
	return planner.NewGoPlanner(llmPlanner)
}

func newLLMPlannerFromConfigMap(ctx context.Context, reader client.Reader, key types.NamespacedName) (*planner.LLMPlanner, planner.Config, error) {
	config, llmClient, err := planner.LoadConfigMap(ctx, reader, key)
	if err != nil {
		return nil, planner.Config{}, err
	}
	return planner.NewLLMPlanner(llmClient), config, nil
}

func newPlanRetrieverFromConfig(config planner.Config) (*rag.PlanRetriever, error) {
	embedder, err := rag.NewOpenAIEmbedder(
		config.EmbeddingBaseURL,
		config.EmbeddingAPIKey,
		config.EmbeddingModel,
	)
	if err != nil {
		return nil, err
	}
	probeVec, err := embedder.Embed(context.Background(), "probe")
	if err != nil {
		return nil, fmt.Errorf("probe embedding dims: %w", err)
	}
	vectorDB := rag.NewMemoryVectorDB(len(probeVec))
	return rag.NewPlanRetriever(vectorDB, embedder), nil
}
