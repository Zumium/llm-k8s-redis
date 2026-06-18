package main

import (
	"flag"
	"os"

	"k8s.io/apimachinery/pkg/runtime"
	"k8s.io/apimachinery/pkg/types"
	utilruntime "k8s.io/apimachinery/pkg/util/runtime"
	clientgoscheme "k8s.io/client-go/kubernetes/scheme"
	_ "k8s.io/client-go/plugin/pkg/client/auth"
	ctrl "sigs.k8s.io/controller-runtime"
	"sigs.k8s.io/controller-runtime/pkg/healthz"
	"sigs.k8s.io/controller-runtime/pkg/log/zap"
	metricsserver "sigs.k8s.io/controller-runtime/pkg/metrics/server"

	api "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/controller"
	"github.com/example/llm-k8s-redis/internal/llm"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/planner"
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
		zapOpts           zap.Options
		metricsAddr       string
		probeAddr         string
		enableLeaderElec  bool
		llmConfigMapName  string
		llmConfigMapNS    string
		disableLLMPlanner bool
	)
	zapOpts.Development = true
	zapOpts.BindFlags(flag.CommandLine)
	flag.StringVar(&metricsAddr, "metrics-bind-address", ":8080", "Metrics server bind address.")
	flag.StringVar(&probeAddr, "health-probe-bind-address", ":8081", "Health probe bind address.")
	flag.BoolVar(&enableLeaderElec, "leader-elect", false, "Enable leader election.")
	flag.StringVar(&llmConfigMapName, "llm-configmap-name", "llm-config", "Name of the ConfigMap holding LLM connection config.")
	flag.StringVar(&llmConfigMapNS, "llm-configmap-namespace", "redis-cluster-system", "Namespace of the LLM ConfigMap.")
	flag.BoolVar(&disableLLMPlanner, "disable-llm-planner", false, "Use NoopPlanner instead of the ConfigMap-driven LLM planner (for testing).")
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

	var p planner.Planner = planner.NoopPlanner{}
	if !disableLLMPlanner {
		src := llm.NewConfigMapSource(mgr.GetClient(), types.NamespacedName{Name: llmConfigMapName, Namespace: llmConfigMapNS})
		if err := mgr.Add(src); err != nil {
			setupLog.Error(err, "unable to register llm config source")
			os.Exit(1)
		}
		p = planner.NewDynamicPlanner(src)
		setupLog.Info("using llm planner backed by configmap", "name", llmConfigMapName, "namespace", llmConfigMapNS)
	} else {
		setupLog.Info("llm planner disabled; using NoopPlanner")
	}

	if err = (&controller.RedisClusterReconciler{
		Client:    mgr.GetClient(),
		Scheme:    scheme,
		Planner:   p,
		Executor:  controller.NoopExecutor{},
		Validator: plan.NewValidator(),
		Recorder:  mgr.GetEventRecorder("rediscluster-controller"),
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
