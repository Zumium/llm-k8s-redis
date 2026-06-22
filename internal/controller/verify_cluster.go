package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/rediscluster"
)

func (e *ActionExecutor) verifyCluster(ctx context.Context, cluster *v1alpha1.RedisCluster, _ *plan.Plan, step plan.Step) (StepOutcome, error) {
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
	if migrating := rediscluster.MigratingSlots(obs.entries); len(migrating) > 0 {
		return paramErr("cluster has %d slots in migrating/importing state", len(migrating))
	}

	owner, err := rediscluster.SlotOwnership(obs.entries)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("slot ownership inconsistent: %v", err)}, err
	}
	if viol := verifyFullSlotCoverage(owner, obs.entries); viol != "" {
		return paramErr("slot coverage violation: %s", viol)
	}

	masters := rediscluster.HealthyMasters(obs.entries)
	if len(masters) != expectedShards {
		return paramErr("expected %d masters, found %d", expectedShards, len(masters))
	}
	for _, m := range masters {
		replicas := rediscluster.HealthyReplicasOf(obs.entries, m.ID)
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

func firstUnhealthyManagedNode(entries []rediscluster.Entry) string {
	for _, e := range entries {
		if e.HasFlag("handshake") || e.HasFlag("noaddr") {
			continue
		}
		if !e.Healthy() {
			return fmt.Sprintf("id=%s addr=%s flags=%v link=%s", e.ID, e.Addr, e.Flags, e.LinkState)
		}
	}
	return ""
}

func verifyFullSlotCoverage(owner map[int]string, entries []rediscluster.Entry) string {
	mastersByID := map[string]*rediscluster.Entry{}
	for i := range entries {
		if entries[i].IsMaster() {
			mastersByID[entries[i].ID] = &entries[i]
		}
	}
	for s := 0; s <= rediscluster.SlotRangeBound; s++ {
		nodeID, ok := owner[s]
		if !ok {
			return fmt.Sprintf("slot %d unassigned", s)
		}
		m, ok := mastersByID[nodeID]
		if !ok {
			return fmt.Sprintf("slot %d owner %s is not a master", s, nodeID)
		}
		if !m.Healthy() {
			return fmt.Sprintf("slot %d owner %s is not healthy", s, nodeID)
		}
	}
	return ""
}

func firstUnmappedNode(entries []rediscluster.Entry, podsByIP map[string]*corev1.Pod) string {
	for _, e := range entries {
		if e.HasFlag("handshake") || e.HasFlag("noaddr") {
			continue
		}
		ip := rediscluster.IPFromAddr(e.Addr)
		if ip == "" {
			return fmt.Sprintf("id=%s has unparseable addr %q", e.ID, e.Addr)
		}
		if _, ok := podsByIP[ip]; !ok {
			return fmt.Sprintf("id=%s ip=%s not a managed pod", e.ID, ip)
		}
	}
	return ""
}
