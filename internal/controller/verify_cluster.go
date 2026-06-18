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
)

// verifyCluster is the executor for plan.ActionVerifyCluster. It does NOT
// mutate the Redis Cluster topology; it only observes, validates and refreshes
// status.topology + the Healthy condition.
//
// Completion criteria (all must hold):
//   - params match the live RedisCluster spec: expectedShards == spec.shards,
//     expectedReplicasPerShard == spec.replicasPerShard, and the three
//     require* flags are all true
//   - at least one managed Pod is Ready with a PodIP (a usable seed)
//   - the seed Redis responds to PING
//   - CLUSTER INFO reports cluster_state:ok
//   - CLUSTER NODES can be parsed
//   - the number of healthy masters equals expectedShards
//   - every healthy master has exactly expectedReplicasPerShard healthy
//     replicas
//   - every slot 0..16383 is owned by exactly one healthy master
//   - no slot is in a migrating/importing state
//   - every Redis node can be mapped back to a managed K8S Pod by IP
//
// On success the executor rebuilds status.topology from the live Redis state
// and sets the Healthy condition to True. Any transient observation failure
// (no seed, PING fail, CLUSTER INFO/NODES error, non-ok cluster_state) keeps
// the step Running so the reconciler retries; any structural invariant
// violation fails the step.
func (e *ActionExecutor) verifyCluster(ctx context.Context, cluster *v1alpha1.RedisCluster, step plan.Step) (StepOutcome, error) {
	expectedShards, outcome, err, ok := paramInt(step.Params, "expectedShards")
	if !ok {
		return outcome, err
	}
	expectedReplicas, outcome, err, ok := paramInt(step.Params, "expectedReplicasPerShard")
	if !ok {
		return outcome, err
	}
	requireStateOk, outcome, err, ok := requireBool(step.Params, "requireClusterStateOk")
	if !ok {
		return outcome, err
	}
	requireFullCoverage, outcome, err, ok := requireBool(step.Params, "requireFullSlotCoverage")
	if !ok {
		return outcome, err
	}
	requireAllOwnersHaveReplicas, outcome, err, ok := requireBool(step.Params, "requireAllSlotOwnersHaveReplicas")
	if !ok {
		return outcome, err
	}

	if expectedShards != int(cluster.Spec.Shards) {
		return paramErr("expectedShards %d != spec.shards %d", expectedShards, cluster.Spec.Shards)
	}
	if expectedReplicas != int(cluster.Spec.ReplicasPerShard) {
		return paramErr("expectedReplicasPerShard %d != spec.replicasPerShard %d", expectedReplicas, cluster.Spec.ReplicasPerShard)
	}
	if !requireStateOk {
		return paramErr("requireClusterStateOk must be true")
	}
	if !requireFullCoverage {
		return paramErr("requireFullSlotCoverage must be true")
	}
	if !requireAllOwnersHaveReplicas {
		return paramErr("requireAllSlotOwnersHaveReplicas must be true")
	}
	if expectedShards < 1 || expectedReplicas < 1 {
		return paramErr("expectedShards and expectedReplicasPerShard must be >= 1")
	}

	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		return outcome, nil
	}
	if len(pods) == 0 {
		return running("no managed pods found for cluster %s", cluster.Name), nil
	}

	seed, ok := pickSeedPod(pods)
	if !ok {
		return running("no ready managed pod with IP yet for cluster %s", cluster.Name), nil
	}

	addr := podRedisAddr(&seed)
	rc, err := e.RedisFactory(addr)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for seed %s: %v", addr, err)}, err
	}
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		return running("seed redis at %s not reachable: %v", addr, err), nil
	}

	infoRaw, err := rc.ClusterInfo(ctx)
	if err != nil {
		return running("seed redis at %s CLUSTER INFO failed: %v", addr, err), nil
	}
	info := parseClusterInfo(infoRaw)
	if !clusterStateOk(info) {
		return running("cluster_state is %q, expected ok", info["cluster_state"]), nil
	}

	nodesRaw, err := rc.ClusterNodes(ctx)
	if err != nil {
		return running("seed redis at %s CLUSTER NODES failed: %v", addr, err), nil
	}
	entries := parseClusterNodes(nodesRaw)
	if len(entries) == 0 {
		return running("seed redis at %s returned no CLUSTER NODES entries", addr), nil
	}

	if bad := firstUnhealthyManagedNode(entries); bad != "" {
		return paramErr("cluster node not healthy: %s", bad)
	}
	if migrating := migratingSlots(entries); len(migrating) > 0 {
		return paramErr("cluster has %d slots in migrating/importing state", len(migrating))
	}

	owner, err := slotOwnership(entries)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("slot ownership inconsistent: %v", err)}, err
	}
	if viol := verifyFullSlotCoverage(owner, entries); viol != "" {
		return paramErr("slot coverage violation: %s", viol)
	}

	masters := healthyMasters(entries)
	if len(masters) != expectedShards {
		return paramErr("expected %d masters, found %d", expectedShards, len(masters))
	}
	for _, m := range masters {
		replicas := healthyReplicasOf(entries, m.ID)
		if len(replicas) != expectedReplicas {
			return paramErr("master %s has %d healthy replicas, expected %d", m.ID, len(replicas), expectedReplicas)
		}
	}

	podsByIP := mapPodsByIP(pods)
	if unmapped := firstUnmappedNode(entries, podsByIP); unmapped != "" {
		return paramErr("redis node not mapped to any managed pod: %s", unmapped)
	}

	topology := rebuildTopology(entries, pods, podsByIP)
	cluster.Status.Topology = topology
	setCondition(cluster, ConditionHealthy, metav1.ConditionTrue, "ClusterVerified", "cluster matches desired topology")
	return completed("cluster verified: %d masters, %d replicas/master, full slot coverage", expectedShards, expectedReplicas), nil
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

// firstUnhealthyManagedNode returns a short description of the first entry
// that is not in handshake state but is failed or disconnected, or "" if all
// managed nodes are healthy. Handshake/noaddr nodes are ignored: they may
// appear transiently and are not part of the cluster's persistent topology.
func firstUnhealthyManagedNode(entries []clusterNodeEntry) string {
	for _, e := range entries {
		if e.hasFlag("handshake") || e.hasFlag("noaddr") {
			continue
		}
		if !e.healthy() {
			return fmt.Sprintf("id=%s addr=%s flags=%v link=%s", e.ID, e.Addr, e.Flags, e.LinkState)
		}
	}
	return ""
}

// verifyFullSlotCoverage checks that every slot 0..16383 is owned and that
// the owner is a healthy master. It returns a non-empty violation message on
// the first problem, or "" if coverage is complete.
func verifyFullSlotCoverage(owner map[int]string, entries []clusterNodeEntry) string {
	mastersByID := map[string]*clusterNodeEntry{}
	for i := range entries {
		if entries[i].isMaster() {
			mastersByID[entries[i].ID] = &entries[i]
		}
	}
	for s := 0; s <= slotRangeBound; s++ {
		nodeID, ok := owner[s]
		if !ok {
			return fmt.Sprintf("slot %d unassigned", s)
		}
		m, ok := mastersByID[nodeID]
		if !ok {
			return fmt.Sprintf("slot %d owner %s is not a master", s, nodeID)
		}
		if !m.healthy() {
			return fmt.Sprintf("slot %d owner %s is not healthy", s, nodeID)
		}
	}
	return ""
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

// firstUnmappedNode returns a description of the first non-handshake cluster
// node whose IP does not correspond to any managed Pod, or "" if all are
// mapped. This guards against foreign nodes leaking into the cluster.
func firstUnmappedNode(entries []clusterNodeEntry, podsByIP map[string]*corev1.Pod) string {
	for _, e := range entries {
		if e.hasFlag("handshake") || e.hasFlag("noaddr") {
			continue
		}
		ip := ipFromAddr(e.Addr)
		if ip == "" {
			return fmt.Sprintf("id=%s has unparseable addr %q", e.ID, e.Addr)
		}
		if _, ok := podsByIP[ip]; !ok {
			return fmt.Sprintf("id=%s ip=%s not a managed pod", e.ID, ip)
		}
	}
	return ""
}

// ipFromAddr extracts the host portion of an "ip:port@cport" address.
func ipFromAddr(addr string) string {
	if i := strings.Index(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
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
