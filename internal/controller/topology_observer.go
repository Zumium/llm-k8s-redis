package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/apimachinery/pkg/labels"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/redis"
)

// Observer is a minimal read-only surface for refreshing the observed Redis
// Cluster topology. It is separate from Executor so the reconciler can ask for
// a topology snapshot without executing a plan step.
type Observer interface {
	ObserveTopology(ctx context.Context, cluster *v1alpha1.RedisCluster) error
}

// noopObserver is the default Observer used when none is wired. It performs no
// Redis calls and leaves status untouched.
type noopObserver struct{}

func (noopObserver) ObserveTopology(_ context.Context, _ *v1alpha1.RedisCluster) error {
	return nil
}

// compile-time check that ActionExecutor satisfies Observer.
var _ Observer = &ActionExecutor{}

// clusterObservation is the result of a single observeTopology call. It holds
// both the rebuilt topology and the raw intermediate state needed by callers
// that perform additional validation (VerifyCluster).
type clusterObservation struct {
	entries  []clusterNodeEntry
	topology *v1alpha1.ClusterTopology
	pods     []corev1.Pod
	podsByIP map[string]*corev1.Pod
	healthy  bool
	message  string
}

// ObserveTopology is the public entry point for lazy topology refresh. It
// mutates cluster.Status to reflect the latest observed topology and the
// Healthy condition. Transient observation failures keep the previous topology
// intact so the last known state is not erased.
func (e *ActionExecutor) ObserveTopology(ctx context.Context, cluster *v1alpha1.RedisCluster) error {
	obs, err := e.observeTopology(ctx, cluster)
	if err != nil {
		return err
	}

	now := metav1.Now()
	cluster.Status.TopologyObservedAt = now

	if !obs.healthy {
		setCondition(cluster, ConditionHealthy, metav1.ConditionFalse, "TopologyObserveFailed", obs.message)
		return nil
	}

	cluster.Status.Topology = obs.topology
	setCondition(cluster, ConditionHealthy, metav1.ConditionTrue, "TopologyObserved", "topology refreshed from live cluster")
	return nil
}

// observeTopology queries a live Redis node and rebuilds ClusterTopology from
// CLUSTER INFO / CLUSTER NODES plus the managed K8S Pods. It is intentionally
// lenient: transient problems (no seed, network blip, cluster_state not ok)
// return healthy=false with a reason message rather than an error. Only
// non-transient failures (e.g. unable to build the Redis client or list Pods)
// are returned as errors.
func (e *ActionExecutor) observeTopology(ctx context.Context, cluster *v1alpha1.RedisCluster) (clusterObservation, error) {
	if e.RedisFactory == nil {
		e.RedisFactory = redis.DefaultFactory
	}

	var obs clusterObservation

	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		return obs, fmt.Errorf("%s", outcome.Message)
	}
	obs.pods = pods
	if len(pods) == 0 {
		obs.message = fmt.Sprintf("no managed pods found for cluster %s", cluster.Name)
		return obs, nil
	}
	obs.podsByIP = mapPodsByIP(pods)

	seed, ok := pickSeedPod(pods)
	if !ok {
		obs.message = fmt.Sprintf("no ready managed pod with IP yet for cluster %s", cluster.Name)
		return obs, nil
	}

	addr := podRedisAddr(&seed)
	rc, err := e.RedisFactory(addr)
	if err != nil {
		return obs, fmt.Errorf("build redis client for seed %s: %w", addr, err)
	}
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		obs.message = fmt.Sprintf("seed redis at %s not reachable: %v", addr, err)
		return obs, nil
	}

	infoRaw, err := rc.ClusterInfo(ctx)
	if err != nil {
		obs.message = fmt.Sprintf("seed redis at %s CLUSTER INFO failed: %v", addr, err)
		return obs, nil
	}
	info := parseClusterInfo(infoRaw)
	if !clusterStateOk(info) {
		obs.message = fmt.Sprintf("cluster_state is %q, expected ok", info["cluster_state"])
		return obs, nil
	}

	nodesRaw, err := rc.ClusterNodes(ctx)
	if err != nil {
		obs.message = fmt.Sprintf("seed redis at %s CLUSTER NODES failed: %v", addr, err)
		return obs, nil
	}
	entries := parseClusterNodes(nodesRaw)
	if len(entries) == 0 {
		obs.message = fmt.Sprintf("seed redis at %s returned no CLUSTER NODES entries", addr)
		return obs, nil
	}

	obs.entries = entries
	obs.topology = rebuildTopology(entries, pods, obs.podsByIP)
	obs.healthy = true
	return obs, nil
}

// listManagedPods lists all Pods in the cluster's namespace labeled as
// belonging to this RedisCluster. It returns a Failed outcome if the list
// call errors for a non-transient reason.
func (e *ActionExecutor) listManagedPods(ctx context.Context, cluster *v1alpha1.RedisCluster) ([]corev1.Pod, StepOutcome, bool) {
	var podList corev1.PodList
	selector, err := labels.Parse(labelCluster + "=" + cluster.Name)
	if err != nil {
		o, _ := paramErr("build pod label selector: %v", err)
		return nil, o, false
	}
	if err := e.List(ctx, &podList, &client.ListOptions{
		Namespace:     cluster.Name,
		LabelSelector: selector,
	}); err != nil {
		if apierrors.IsNotFound(err) {
			return nil, StepOutcome{Status: plan.StepStateRunning, Message: fmt.Sprintf("namespace %s not found yet", cluster.Name)}, true
		}
		o, _ := paramErr("list managed pods: %v", err)
		return nil, o, false
	}
	return podList.Items, StepOutcome{}, true
}

// pickSeedPod returns the first managed Pod that is Ready and has a PodIP.
func pickSeedPod(pods []corev1.Pod) (corev1.Pod, bool) {
	for _, p := range pods {
		if podReady(&p) && p.Status.PodIP != "" {
			return p, true
		}
	}
	return corev1.Pod{}, false
}

// mapPodsByIP indexes ready pods by their PodIP for Redis-node -> Pod lookup.
func mapPodsByIP(pods []corev1.Pod) map[string]*corev1.Pod {
	out := map[string]*corev1.Pod{}
	for i := range pods {
		if pods[i].Status.PodIP == "" {
			continue
		}
		out[pods[i].Status.PodIP] = &pods[i]
	}
	return out
}

// healthyMasters returns the entries that are masters and healthy, sorted by
// node id for deterministic shard numbering.
func healthyMasters(entries []clusterNodeEntry) []clusterNodeEntry {
	var out []clusterNodeEntry
	for _, e := range entries {
		if e.isMaster() && e.healthy() {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// healthyReplicasOf returns the entries that are healthy replicas of the
// given master id, sorted by node id for deterministic ordering.
func healthyReplicasOf(entries []clusterNodeEntry, masterID string) []clusterNodeEntry {
	var out []clusterNodeEntry
	for _, e := range entries {
		if e.isReplica() && e.MasterID == masterID && e.healthy() {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// rebuildTopology reconstructs ClusterTopology from live Redis CLUSTER NODES
// plus the managed K8S Pods. Masters are sorted by node id to give stable
// shard indexes; replicas are sorted by node id within each shard. Slot
// tokens are joined with "," so the Slots field reads like "0-8191" or
// "0-100,5000".
func rebuildTopology(entries []clusterNodeEntry, pods []corev1.Pod, podsByIP map[string]*corev1.Pod) *v1alpha1.ClusterTopology {
	masters := healthyMasters(entries)
	shards := make([]v1alpha1.ShardTopology, 0, len(masters))
	for idx, m := range masters {
		shard := v1alpha1.ShardTopology{
			ID: fmt.Sprintf("shard-%d", idx),
			Master: v1alpha1.NodeTopology{
				Pod:    podNameForIP(podsByIP, ipFromAddr(m.Addr)),
				NodeID: m.ID,
				Slots:  strings.Join(m.Slots, ","),
				Ready:  podReadyForIP(podsByIP, ipFromAddr(m.Addr)),
			},
		}
		for _, r := range healthyReplicasOf(entries, m.ID) {
			ip := ipFromAddr(r.Addr)
			shard.Replicas = append(shard.Replicas, v1alpha1.NodeTopology{
				Pod:    podNameForIP(podsByIP, ip),
				NodeID: r.ID,
				Ready:  podReadyForIP(podsByIP, ip),
			})
		}
		shards = append(shards, shard)
	}
	return &v1alpha1.ClusterTopology{Shards: shards}
}

func podNameForIP(podsByIP map[string]*corev1.Pod, ip string) string {
	if p, ok := podsByIP[ip]; ok {
		return p.Name
	}
	return ""
}

func podReadyForIP(podsByIP map[string]*corev1.Pod, ip string) bool {
	if p, ok := podsByIP[ip]; ok {
		return podReady(p)
	}
	return false
}

// ipFromAddr extracts the host portion of an "ip:port@cport" address.
func ipFromAddr(addr string) string {
	if i := strings.Index(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
}
