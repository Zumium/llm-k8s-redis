package controller

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/rediscluster"
)

const (
	verifyStableRequired = 2
	verifyStableTimeout  = 2 * time.Minute

	verifyStartedAtKey   = "__controllerVerifyStartedAt"
	verifyFingerprintKey = "__controllerVerifyFingerprint"
	verifyStableCountKey = "__controllerVerifyStableCount"
)

func (e *ActionExecutor) verifyCluster(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	start := time.Now()
	logger := log.FromContext(ctx)
	logger.Info("verify cluster started")
	step := &p.Steps[stepIndex]
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
		outcome, _, err := waitForStableCluster(step.Params, start, "", obs.message)
		return outcome, err
	}
	logger.Info("verify cluster topology observed", "duration", time.Since(start), "entries", len(obs.entries), "shards", len(obs.topology.Shards))

	outcome, stable, err := waitForStableCluster(step.Params, start, clusterObservationFingerprint(obs), "")
	if err != nil || !stable {
		return outcome, err
	}

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

func waitForStableCluster(params map[string]any, now time.Time, fingerprint, unhealthyMessage string) (StepOutcome, bool, error) {
	started := verifyStartedAt(params, now)
	params[verifyStartedAtKey] = started.Format(time.RFC3339Nano)

	count := 0
	if fingerprint != "" {
		count = 1
		if paramStringValue(params, verifyFingerprintKey) == fingerprint {
			count = paramIntValue(params, verifyStableCountKey) + 1
		}
		params[verifyFingerprintKey] = fingerprint
		params[verifyStableCountKey] = count
	}

	if count >= verifyStableRequired {
		return StepOutcome{}, true, nil
	}
	if now.Sub(started) >= verifyStableTimeout {
		if unhealthyMessage != "" {
			outcome, err := paramErr("cluster did not stabilize within %s: %s", verifyStableTimeout, unhealthyMessage)
			return outcome, false, err
		}
		outcome, err := paramErr("cluster did not stabilize within %s", verifyStableTimeout)
		return outcome, false, err
	}
	if unhealthyMessage != "" {
		return running("%s", unhealthyMessage), false, nil
	}
	out := running("waiting for stable cluster observation: %d/%d", count, verifyStableRequired)
	return out, false, nil
}

func verifyStartedAt(params map[string]any, now time.Time) time.Time {
	if s := paramStringValue(params, verifyStartedAtKey); s != "" {
		if t, err := time.Parse(time.RFC3339Nano, s); err == nil {
			return t
		}
	}
	return now
}

func clusterObservationFingerprint(obs clusterObservation) string {
	parts := make([]string, 0, len(obs.entries)+len(obs.pods))
	for _, e := range obs.entries {
		flags := append([]string{}, e.Flags...)
		sort.Strings(flags)
		slots := append([]string{}, e.Slots...)
		sort.Strings(slots)
		parts = append(parts, "node|"+strings.Join([]string{
			e.ID,
			e.Addr,
			strings.Join(flags, ","),
			e.MasterID,
			e.LinkState,
			strings.Join(slots, ","),
		}, "|"))
	}
	for _, p := range obs.pods {
		parts = append(parts, fmt.Sprintf("pod|%s|%s|%t", p.Name, p.Status.PodIP, podReady(&p)))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

func paramStringValue(params map[string]any, key string) string {
	v, ok := params[key].(string)
	if !ok {
		return ""
	}
	return v
}

func paramIntValue(params map[string]any, key string) int {
	v, _, _, ok := paramInt(params, key)
	if !ok {
		return 0
	}
	return v
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
