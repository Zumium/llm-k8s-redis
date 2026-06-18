package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

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

	obs, err := e.observeTopology(ctx, cluster)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: err.Error()}, err
	}
	if !obs.healthy {
		return running("%s", obs.message), nil
	}

	if bad := firstUnhealthyManagedNode(obs.entries); bad != "" {
		return paramErr("cluster node not healthy: %s", bad)
	}
	if migrating := migratingSlots(obs.entries); len(migrating) > 0 {
		return paramErr("cluster has %d slots in migrating/importing state", len(migrating))
	}

	owner, err := slotOwnership(obs.entries)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("slot ownership inconsistent: %v", err)}, err
	}
	if viol := verifyFullSlotCoverage(owner, obs.entries); viol != "" {
		return paramErr("slot coverage violation: %s", viol)
	}

	masters := healthyMasters(obs.entries)
	if len(masters) != expectedShards {
		return paramErr("expected %d masters, found %d", expectedShards, len(masters))
	}
	for _, m := range masters {
		replicas := healthyReplicasOf(obs.entries, m.ID)
		if len(replicas) != expectedReplicas {
			return paramErr("master %s has %d healthy replicas, expected %d", m.ID, len(replicas), expectedReplicas)
		}
	}

	if unmapped := firstUnmappedNode(obs.entries, obs.podsByIP); unmapped != "" {
		return paramErr("redis node not mapped to any managed pod: %s", unmapped)
	}

	cluster.Status.Topology = obs.topology
	setCondition(cluster, ConditionHealthy, metav1.ConditionTrue, "ClusterVerified", "cluster matches desired topology")
	return completed("cluster verified: %d masters, %d replicas/master, full slot coverage", expectedShards, expectedReplicas), nil
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
