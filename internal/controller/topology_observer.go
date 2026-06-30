package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/redis"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
)

type clusterObservation struct {
	entries  []rediscluster.Entry
	views    []clusterView
	topology *v1alpha1.ClusterTopology
	pods     []corev1.Pod
	podsByIP map[string]*corev1.Pod
	healthy  bool
	message  string
}

type clusterView struct {
	Seed    string
	Entries []rediscluster.Entry
}

func (e *ActionExecutor) ObserveTopology(ctx context.Context, cluster *v1alpha1.RedisCluster) error {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)
	logger.Info("observe topology started")
	obs, err := e.observeTopology(ctx, cluster)
	if err != nil {
		logger.Error(err, "observe topology failed", "duration", time.Since(start))
		return err
	}

	now := metav1.Now()
	cluster.Status.TopologyObservedAt = now

	if !obs.healthy {
		logger.Info("observe topology unhealthy", "duration", time.Since(start), "pods", len(obs.pods), "entries", len(obs.entries), "message", obs.message)
		setCondition(cluster, ConditionHealthy, metav1.ConditionFalse, "TopologyObserveFailed", obs.message)
		return nil
	}

	cluster.Status.Topology = obs.topology
	nodes := observedNodes(cluster, obs.pods, obs.entries)
	cluster.Status.ObservedNodes = apiObservedNodes(nodes)
	bumpNextPodOrdinalFromObserved(cluster, nodes)
	setCondition(cluster, ConditionHealthy, metav1.ConditionTrue, "TopologyObserved", "topology refreshed from live cluster")
	logger.Info("observe topology finished", "duration", time.Since(start), "pods", len(obs.pods), "entries", len(obs.entries), "shards", len(obs.topology.Shards))
	return nil
}

func (e *ActionExecutor) observeTopology(ctx context.Context, cluster *v1alpha1.RedisCluster) (clusterObservation, error) {
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)
	if e.RedisFactory == nil {
		e.RedisFactory = redis.DefaultFactory
		logger.Info("default redis factory initialized")
	}

	var obs clusterObservation

	listStart := time.Now()
	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		logger.Info("list managed pods failed", "duration", time.Since(listStart), "message", outcome.Message)
		return obs, fmt.Errorf("%s", outcome.Message)
	}
	logger.Info("managed pods listed", "duration", time.Since(listStart), "pods", len(pods))
	obs.pods = pods
	if len(pods) == 0 {
		obs.message = fmt.Sprintf("no managed pods found for cluster %s", cluster.Name)
		logger.Info("observe topology has no pods")
		return obs, nil
	}
	obs.podsByIP = mapPodsByIP(pods)

	seed, ok := pickObservationSeedPod(cluster, pods)
	if !ok {
		obs.message = fmt.Sprintf("no ready managed pod with IP yet for cluster %s", cluster.Name)
		logger.Info("observe topology has no seed pod")
		return obs, nil
	}
	logger.Info("observe topology seed selected", "pod", seed.Name, "ip", seed.Status.PodIP)

	addr := podRedisAddr(&seed)
	rc, err := e.RedisFactory(addr)
	if err != nil {
		logger.Error(err, "build redis client for topology seed failed", "addr", addr)
		return obs, fmt.Errorf("build redis client for seed %s: %w", addr, err)
	}
	defer rc.Close()

	pingStart := time.Now()
	if err := rc.Ping(ctx); err != nil {
		obs.message = fmt.Sprintf("seed redis at %s not reachable: %v", addr, err)
		logger.Info("topology seed ping failed", "addr", addr, "duration", time.Since(pingStart), "error", err)
		return obs, nil
	}
	logger.Info("topology seed ping succeeded", "addr", addr, "duration", time.Since(pingStart))

	infoStart := time.Now()
	infoRaw, err := rc.ClusterInfo(ctx)
	if err != nil {
		obs.message = fmt.Sprintf("seed redis at %s CLUSTER INFO failed: %v", addr, err)
		logger.Info("cluster info failed", "addr", addr, "duration", time.Since(infoStart), "error", err)
		return obs, nil
	}
	logger.Info("cluster info read", "addr", addr, "duration", time.Since(infoStart))
	info := rediscluster.ParseInfo(infoRaw)
	if !rediscluster.StateOK(info) {
		obs.message = fmt.Sprintf("cluster_state is %q, expected ok", info["cluster_state"])
		logger.Info("cluster state not ok", "state", info["cluster_state"])
		return obs, nil
	}

	viewsStart := time.Now()
	views, err := e.collectClusterViews(ctx, observationSeedPods(cluster, pods))
	if err != nil {
		obs.message = err.Error()
		logger.Info("cluster nodes failed", "duration", time.Since(viewsStart), "error", err)
		return obs, nil
	}
	if len(views) == 0 {
		obs.message = fmt.Sprintf("seed redis at %s returned no CLUSTER NODES entries", addr)
		logger.Info("cluster nodes returned no entries", "addr", addr)
		return obs, nil
	}
	entries := bestClusterView(views).Entries
	logger.Info("cluster nodes read", "duration", time.Since(viewsStart), "views", len(views), "entries", len(entries))

	obs.entries = entries
	obs.views = views
	obs.topology = rebuildTopology(entries, pods, obs.podsByIP)
	obs.healthy = true
	logger.Info("topology rebuilt", "shards", len(obs.topology.Shards), "entries", len(entries), "pods", len(pods))
	return obs, nil
}

func (e *ActionExecutor) collectClusterViews(ctx context.Context, seeds []corev1.Pod) ([]clusterView, error) {
	var out []clusterView
	var lastErr error
	for _, seed := range seeds {
		entries, err := e.clusterNodesFromSeed(ctx, seed)
		if err != nil {
			lastErr = err
			continue
		}
		if len(entries) == 0 {
			continue
		}
		out = append(out, clusterView{Seed: seed.Name, Entries: entries})
	}
	if len(out) > 0 {
		return out, nil
	}
	if lastErr != nil {
		return nil, lastErr
	}
	return nil, nil
}

func bestClusterView(views []clusterView) clusterView {
	best := views[0]
	for _, v := range views[1:] {
		if len(v.Entries) > len(best.Entries) || (!ownsAllSlots(best.Entries) && ownsAllSlots(v.Entries)) {
			best = v
		}
	}
	return best
}

func (e *ActionExecutor) CollectObservedNodes(ctx context.Context, cluster *v1alpha1.RedisCluster) ([]plan.ObservedNode, error) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name)
	logger.Info("collect observed nodes started")
	if e.RedisFactory == nil {
		e.RedisFactory = redis.DefaultFactory
		logger.Info("default redis factory initialized")
	}
	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		logger.Info("collect observed nodes list pods failed", "duration", time.Since(start), "message", outcome.Message)
		return nil, fmt.Errorf("%s", outcome.Message)
	}
	logger.Info("collect observed nodes pods listed", "pods", len(pods))

	entries := []rediscluster.Entry{}
	seeds := observationSeedPods(cluster, pods)
	if len(seeds) == 0 {
		nodes := observedNodes(cluster, pods, entries)
		logger.Info("collect observed nodes no seed pod", "duration", time.Since(start), "nodes", len(nodes), "pods", len(pods))
		return nodes, nil
	}

	entries, err := e.collectBestClusterNodes(ctx, seeds)
	if err != nil {
		return nil, err
	}
	nodes := observedNodes(cluster, pods, entries)
	logger.Info("collect observed nodes finished", "duration", time.Since(start), "nodes", len(nodes), "pods", len(pods), "entries", len(entries))
	return nodes, nil
}

func (e *ActionExecutor) collectBestClusterNodes(ctx context.Context, seeds []corev1.Pod) ([]rediscluster.Entry, error) {
	best := []rediscluster.Entry{}
	var lastErr error
	for _, seed := range seeds {
		entries, err := e.clusterNodesFromSeed(ctx, seed)
		if err != nil {
			lastErr = err
			continue
		}
		if len(entries) > len(best) {
			best = entries
		}
		if ownsAllSlots(entries) {
			return entries, nil
		}
	}
	if len(best) > 0 {
		return best, nil
	}
	if lastErr == nil {
		return best, nil
	}
	return nil, lastErr
}

func (e *ActionExecutor) clusterNodesFromSeed(ctx context.Context, seed corev1.Pod) ([]rediscluster.Entry, error) {
	logger := log.FromContext(ctx)
	addr := podRedisAddr(&seed)
	logger.Info("collect observed nodes seed selected", "pod", seed.Name, "addr", addr)
	rc, err := e.RedisFactory(addr)
	if err != nil {
		logger.Error(err, "collect observed nodes build redis client failed", "addr", addr)
		return nil, fmt.Errorf("build redis client for seed %s: %w", addr, err)
	}
	defer rc.Close()

	pingStart := time.Now()
	if err := rc.Ping(ctx); err != nil {
		logger.Error(err, "collect observed nodes seed ping failed", "addr", addr, "duration", time.Since(pingStart))
		return nil, fmt.Errorf("seed redis at %s failed: %w", addr, err)
	}
	logger.Info("collect observed nodes seed ping succeeded", "addr", addr, "duration", time.Since(pingStart))

	nodesStart := time.Now()
	raw, err := rc.ClusterNodes(ctx)
	if err != nil {
		logger.Error(err, "collect observed nodes cluster nodes failed", "addr", addr, "duration", time.Since(nodesStart))
		return nil, fmt.Errorf("seed redis at %s failed: %w", addr, err)
	}
	entries := rediscluster.ParseNodes(raw)
	logger.Info("collect observed nodes seed read", "addr", addr, "duration", time.Since(nodesStart), "entries", len(entries))
	return entries, nil
}

func (e *ActionExecutor) listManagedPods(ctx context.Context, cluster *v1alpha1.RedisCluster) ([]corev1.Pod, StepOutcome, bool) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues("rediscluster", cluster.Name, "namespace", cluster.Name)
	var podList corev1.PodList
	logger.Info("listing managed pods", "cluster", cluster.Name)
	if err := e.List(ctx, &podList, &client.ListOptions{
		Namespace: cluster.Name,
	}, client.MatchingLabels{labelCluster: cluster.Name}); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("namespace not found while listing managed pods", "duration", time.Since(start))
			return nil, StepOutcome{Status: plan.StepStateRunning, Message: fmt.Sprintf("namespace %s not found yet", cluster.Name)}, true
		}
		logger.Error(err, "list managed pods failed", "duration", time.Since(start))
		o, _ := paramErr("list managed pods: %v", err)
		return nil, o, false
	}
	logger.Info("managed pods listed", "duration", time.Since(start), "pods", len(podList.Items))
	return podList.Items, StepOutcome{}, true
}

func pickSeedPod(pods []corev1.Pod) (corev1.Pod, bool) {
	for _, p := range pods {
		if podReady(&p) && p.Status.PodIP != "" {
			return p, true
		}
	}
	return corev1.Pod{}, false
}

func pickObservationSeedPod(cluster *v1alpha1.RedisCluster, pods []corev1.Pod) (corev1.Pod, bool) {
	seeds := observationSeedPods(cluster, pods)
	if len(seeds) == 0 {
		return corev1.Pod{}, false
	}
	return seeds[0], true
}

func observationSeedPods(cluster *v1alpha1.RedisCluster, pods []corev1.Pod) []corev1.Pod {
	podsByName := map[string]corev1.Pod{}
	for _, p := range pods {
		podsByName[p.Name] = p
	}
	out := []corev1.Pod{}
	seen := map[string]bool{}
	if cluster != nil && cluster.Status.Topology != nil {
		for _, sh := range cluster.Status.Topology.Shards {
			p, ok := podsByName[sh.Master.Pod]
			if ok && podReady(&p) && p.Status.PodIP != "" {
				out = append(out, p)
				seen[p.Name] = true
			}
		}
	}
	ready := []corev1.Pod{}
	for _, p := range pods {
		if !seen[p.Name] && podReady(&p) && p.Status.PodIP != "" {
			ready = append(ready, p)
		}
	}
	sort.Slice(ready, func(i, j int) bool {
		ai, aok := controllerRedisPodOrdinal(ready[i].Name)
		bi, bok := controllerRedisPodOrdinal(ready[j].Name)
		if aok && bok {
			return ai < bi
		}
		return ready[i].Name < ready[j].Name
	})
	return append(out, ready...)
}

func ownsAllSlots(entries []rediscluster.Entry) bool {
	owners, err := rediscluster.SlotOwnership(entries)
	return err == nil && len(owners) == 16384
}

func observedNodes(cluster *v1alpha1.RedisCluster, pods []corev1.Pod, entries []rediscluster.Entry) []plan.ObservedNode {
	podToNodeID, nodeIDToPod := lastKnownNodeMaps(cluster)
	podsByIP := mapPodsByIP(pods)
	for _, entry := range entries {
		if pod := podNameForIP(podsByIP, rediscluster.IPFromAddr(entry.Addr)); pod != "" {
			podToNodeID[pod] = entry.ID
			nodeIDToPod[entry.ID] = pod
		}
	}
	seen := map[string]bool{}
	out := make([]plan.ObservedNode, 0, len(pods)+len(entries))

	for _, p := range pods {
		n := plan.ObservedNode{
			Pod:       p.Name,
			PodExists: true,
			NodeID:    podToNodeID[p.Name],
			Ready:     podReady(&p),
			Deleting:  p.DeletionTimestamp != nil,
			Role:      "unknown",
		}
		entry := rediscluster.FindByIP(entries, p.Status.PodIP)
		if entry == nil && n.NodeID != "" {
			entry = rediscluster.FindByID(entries, n.NodeID)
		}
		if entry != nil {
			fillObservedNode(&n, *entry, nodeIDToPod)
			seen[entry.ID] = true
		}
		out = append(out, n)
	}

	for _, entry := range entries {
		if seen[entry.ID] {
			continue
		}
		n := plan.ObservedNode{Pod: nodeIDToPod[entry.ID], PodExists: false}
		fillObservedNode(&n, entry, nodeIDToPod)
		out = append(out, n)
	}

	sort.Slice(out, func(i, j int) bool {
		ai, aok := controllerRedisPodOrdinal(out[i].Pod)
		bi, bok := controllerRedisPodOrdinal(out[j].Pod)
		if aok && bok {
			return ai < bi
		}
		if aok != bok {
			return aok
		}
		if out[i].Pod != out[j].Pod {
			return out[i].Pod < out[j].Pod
		}
		return out[i].NodeID < out[j].NodeID
	})
	return out
}

func fillObservedNode(n *plan.ObservedNode, entry rediscluster.Entry, nodeIDToPod map[string]string) {
	n.RedisSeen = true
	n.NodeID = entry.ID
	n.Role = redisRole(entry)
	n.Slots = strings.Join(entry.Slots, ",")
	if entry.MasterID != "-" {
		n.MasterID = entry.MasterID
		n.MasterPod = nodeIDToPod[entry.MasterID]
	}
	n.Flags = append([]string{}, entry.Flags...)
	n.LinkState = entry.LinkState
}

func redisRole(entry rediscluster.Entry) string {
	switch {
	case entry.IsMaster():
		return "master"
	case entry.IsReplica():
		return "replica"
	default:
		return "unknown"
	}
}

func lastKnownNodeMaps(cluster *v1alpha1.RedisCluster) (map[string]string, map[string]string) {
	podToNodeID := map[string]string{}
	nodeIDToPod := map[string]string{}
	if cluster == nil || cluster.Status.Topology == nil {
		return podToNodeID, nodeIDToPod
	}
	add := func(n v1alpha1.NodeTopology) {
		if n.Pod == "" || n.NodeID == "" {
			return
		}
		podToNodeID[n.Pod] = n.NodeID
		nodeIDToPod[n.NodeID] = n.Pod
	}
	for _, sh := range cluster.Status.Topology.Shards {
		add(sh.Master)
		for _, r := range sh.Replicas {
			add(r)
		}
	}
	return podToNodeID, nodeIDToPod
}

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

func rebuildTopology(entries []rediscluster.Entry, pods []corev1.Pod, podsByIP map[string]*corev1.Pod) *v1alpha1.ClusterTopology {
	masters := rediscluster.HealthyMasters(entries)
	shards := make([]v1alpha1.ShardTopology, 0, len(masters))
	for idx, m := range masters {
		shard := v1alpha1.ShardTopology{
			ID: fmt.Sprintf("shard-%d", idx),
			Master: v1alpha1.NodeTopology{
				Pod:    podNameForIP(podsByIP, rediscluster.IPFromAddr(m.Addr)),
				NodeID: m.ID,
				Slots:  strings.Join(m.Slots, ","),
				Ready:  podReadyForIP(podsByIP, rediscluster.IPFromAddr(m.Addr)),
			},
		}
		for _, r := range rediscluster.HealthyReplicasOf(entries, m.ID) {
			ip := rediscluster.IPFromAddr(r.Addr)
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
