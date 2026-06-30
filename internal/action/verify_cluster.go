package action

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	corev1 "k8s.io/api/core/v1"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
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

	state, message := classifyClusterView(obs, expectedShards, expectedReplicas)
	if state == clusterViewConverging {
		logger.Info("verify cluster waiting for convergence", "message", message)
		return running("waiting for gossip to converge: %s", message), nil
	}

	outcome, stable, err := waitForStableCluster(step.Params, start, clusterObservationFingerprint(obs), "")
	if err != nil || !stable {
		return outcome, err
	}
	if state == clusterViewNeedsRepair {
		logger.Info("verify cluster needs repair", "message", message)
		return needsRepair("%s", message), nil
	}

	cluster.Status.Topology = obs.topology
	setCondition(cluster, ConditionHealthy, metav1.ConditionTrue, "ClusterVerified", "cluster matches desired topology")
	logger.Info("verify cluster finished", "duration", time.Since(start), "masters", expectedShards, "replicasPerMaster", expectedReplicas)
	return completed("cluster verified: %d masters, %d replicas/master, full slot coverage", expectedShards, expectedReplicas), nil
}

type clusterViewState string

const (
	clusterViewConverging  clusterViewState = "Converging"
	clusterViewNeedsRepair clusterViewState = "NeedsRepair"
	clusterViewConverged   clusterViewState = "Converged"
)

func classifyClusterView(obs clusterObservation, expectedShards, expectedReplicas int) (clusterViewState, string) {
	if clusterViewsDiffer(obs.views) {
		return clusterViewConverging, "managed pods report different cluster views"
	}
	for _, e := range obs.entries {
		if e.HasFlag("handshake") || e.HasFlag("noaddr") {
			return clusterViewConverging, fmt.Sprintf("node still handshaking: id=%s addr=%s flags=%v", e.ID, e.Addr, e.Flags)
		}
		if e.HasFlag("fail") || e.HasFlag("fail?") {
			return clusterViewNeedsRepair, fmt.Sprintf("cluster node failed: id=%s addr=%s flags=%v link=%s", e.ID, e.Addr, e.Flags, e.LinkState)
		}
		if !e.IsConnected() {
			return clusterViewConverging, fmt.Sprintf("node link not connected: id=%s addr=%s flags=%v link=%s", e.ID, e.Addr, e.Flags, e.LinkState)
		}
	}
	if migrating := rediscluster.MigratingSlots(obs.entries); len(migrating) > 0 {
		return clusterViewNeedsRepair, fmt.Sprintf("cluster has %d slots in migrating/importing state", len(migrating))
	}
	owner, err := rediscluster.SlotOwnership(obs.entries)
	if err != nil {
		return clusterViewNeedsRepair, fmt.Sprintf("slot ownership inconsistent: %v", err)
	}
	if viol := verifyFullSlotCoverage(owner, obs.entries); viol != "" {
		return clusterViewNeedsRepair, fmt.Sprintf("slot coverage violation: %s", viol)
	}
	masters := rediscluster.HealthyMasters(obs.entries)
	slotMasters := slotOwningMasters(masters)
	if len(masters) != len(slotMasters) {
		return clusterViewConverging, fmt.Sprintf("expected %d masters, found %d healthy masters including %d no-slot masters", expectedShards, len(masters), len(masters)-len(slotMasters))
	}
	if len(slotMasters) != expectedShards {
		return clusterViewNeedsRepair, fmt.Sprintf("expected %d slot-owning masters, found %d", expectedShards, len(slotMasters))
	}
	for _, m := range slotMasters {
		replicas := rediscluster.HealthyReplicasOf(obs.entries, m.ID)
		if len(replicas) != expectedReplicas {
			return clusterViewNeedsRepair, fmt.Sprintf("master %s has %d healthy replicas, expected %d", m.ID, len(replicas), expectedReplicas)
		}
	}
	if unmapped := firstUnmappedNode(obs.entries, obs.podsByIP); unmapped != "" {
		return clusterViewNeedsRepair, fmt.Sprintf("redis node not mapped to any managed pod: %s", unmapped)
	}
	return clusterViewConverged, ""
}

func clusterViewsDiffer(views []clusterView) bool {
	if len(views) < 2 {
		return false
	}
	first := entriesFingerprint(views[0].Entries)
	for _, v := range views[1:] {
		if entriesFingerprint(v.Entries) != first {
			return true
		}
	}
	return false
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
	parts := make([]string, 0, len(obs.views)+len(obs.pods)+1)
	parts = append(parts, "best|"+entriesFingerprint(obs.entries))
	for _, v := range obs.views {
		parts = append(parts, "view|"+v.Seed+"|"+entriesFingerprint(v.Entries))
	}
	for _, p := range obs.pods {
		parts = append(parts, fmt.Sprintf("pod|%s|%s|%t", p.Name, p.Status.PodIP, podReady(&p)))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

func entriesFingerprint(entries []rediscluster.Entry) string {
	parts := make([]string, 0, len(entries))
	for _, e := range entries {
		flags := stableFlags(e.Flags)
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
	sort.Strings(parts)
	return strings.Join(parts, "\n")
}

func stableFlags(flags []string) []string {
	out := make([]string, 0, len(flags))
	for _, f := range flags {
		if strings.EqualFold(f, "myself") {
			continue
		}
		out = append(out, f)
	}
	return out
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

func slotOwningMasters(masters []rediscluster.Entry) []rediscluster.Entry {
	out := make([]rediscluster.Entry, 0, len(masters))
	for _, m := range masters {
		if m.HasSlots() {
			out = append(out, m)
		}
	}
	return out
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
