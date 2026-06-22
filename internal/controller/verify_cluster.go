package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/rediscluster"
)

func (e *ActionExecutor) verifyCluster(ctx context.Context, cluster *v1alpha1.RedisCluster, _ *plan.Plan, step plan.Step) (StepOutcome, error) {
	start := time.Now()
	logger := log.FromContext(ctx)
	logger.Info("verify cluster started")
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
	logger.Info("verify cluster parameters accepted", "expectedShards", expectedShards, "expectedReplicasPerShard", expectedReplicas)

	obs, err := e.observeTopology(ctx, cluster)
	if err != nil {
		logger.Error(err, "verify cluster observe topology failed", "duration", time.Since(start))
		return StepOutcome{Status: plan.StepStateFailed, Message: err.Error()}, err
	}
	if !obs.healthy {
		logger.Info("verify cluster observe topology unhealthy", "duration", time.Since(start), "message", obs.message)
		return running("%s", obs.message), nil
	}
	logger.Info("verify cluster topology observed", "duration", time.Since(start), "entries", len(obs.entries), "shards", len(obs.topology.Shards))

	if bad := firstUnhealthyManagedNode(obs.entries); bad != "" {
		logger.Info("verify cluster found unhealthy node", "node", bad)
		return paramErr("cluster node not healthy: %s", bad)
	}
	if migrating := rediscluster.MigratingSlots(obs.entries); len(migrating) > 0 {
		logger.Info("verify cluster found migrating slots", "slots", len(migrating))
		return paramErr("cluster has %d slots in migrating/importing state", len(migrating))
	}

	owner, err := rediscluster.SlotOwnership(obs.entries)
	if err != nil {
		logger.Error(err, "verify cluster slot ownership failed")
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("slot ownership inconsistent: %v", err)}, err
	}
	if viol := verifyFullSlotCoverage(owner, obs.entries); viol != "" {
		logger.Info("verify cluster slot coverage violation", "violation", viol)
		return paramErr("slot coverage violation: %s", viol)
	}
	logger.Info("verify cluster slot coverage passed", "slots", len(owner))

	masters := rediscluster.HealthyMasters(obs.entries)
	if len(masters) != expectedShards {
		logger.Info("verify cluster master count mismatch", "expected", expectedShards, "got", len(masters))
		return paramErr("expected %d masters, found %d", expectedShards, len(masters))
	}
	for _, m := range masters {
		replicas := rediscluster.HealthyReplicasOf(obs.entries, m.ID)
		if len(replicas) != expectedReplicas {
			logger.Info("verify cluster replica count mismatch", "masterID", m.ID, "expected", expectedReplicas, "got", len(replicas))
			return paramErr("master %s has %d healthy replicas, expected %d", m.ID, len(replicas), expectedReplicas)
		}
	}
	logger.Info("verify cluster shard layout passed", "masters", len(masters), "replicasPerMaster", expectedReplicas)

	if unmapped := firstUnmappedNode(obs.entries, obs.podsByIP); unmapped != "" {
		logger.Info("verify cluster found unmapped node", "node", unmapped)
		return paramErr("redis node not mapped to any managed pod: %s", unmapped)
	}

	cluster.Status.Topology = obs.topology
	setCondition(cluster, ConditionHealthy, metav1.ConditionTrue, "ClusterVerified", "cluster matches desired topology")
	logger.Info("verify cluster finished", "duration", time.Since(start), "masters", expectedShards, "replicasPerMaster", expectedReplicas)
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
