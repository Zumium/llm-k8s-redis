package controller

import (
	"context"
	"fmt"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
)

// addSlotsBatchSize is the maximum number of slots passed to a single
// `CLUSTER ADDSLOTS` call. Redis accepts many slots per call but keeping the
// batch bounded avoids building huge command argument lists and lets the
// controller report partial progress on failure.
const addSlotsBatchSize = 512

// addSlots is the executor for plan.ActionAddSlots. It is idempotent and
// re-derives state from K8S and Redis on every call.
//
// Completion criteria:
//   - the target Pod exists with an IP
//   - the target Pod has been declared by a preceding EnsureNode and passed
//     WaitNodeReady
//   - namespace == cluster name, slots parse and are in [0, 16383]
//   - the target node is visible in CLUSTER NODES and is a master
//   - the target master has at least one healthy replica
//   - every desired slot either already belongs to the target master or is
//     unassigned; after issuing `CLUSTER ADDSLOTS` for the unassigned ones,
//     re-querying CLUSTER NODES confirms they all belong to the target
//     master.
//
// Safety invariants:
//   - if any desired slot already belongs to a different master, the step
//     fails. Slot migration is the responsibility of MigrateSlots, not
//     AddSlots.
//   - if any desired slot is currently in a migrating/importing state, the
//     step fails: such slots must be resolved before ownership can be
//     claimed.
//   - if the target node is a replica, the step fails.
//
// Topology: addSlots refreshes NodeID/Ready/Slots on any existing
// status.topology entry that references the target Pod, but does not invent
// shard structure. Full topology reconstruction is left to VerifyCluster.
func (e *ActionExecutor) addSlots(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	ns, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "namespace")
	if !ok {
		return outcome, err
	}
	podName, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "pod")
	if !ok {
		return outcome, err
	}
	slotsSpec, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "slots")
	if !ok {
		return outcome, err
	}

	if ns != cluster.Name {
		return paramErr("namespace %q must equal cluster name %q", ns, cluster.Name)
	}
	if !precededEnsureNode(p, stepIndex, ns, podName) {
		return paramErr("pod %s/%s was not declared by a preceding EnsureNode", ns, podName)
	}
	if !precededWaitNodeReady(p, stepIndex, ns, podName) {
		return paramErr("pod %s/%s has not completed a preceding WaitNodeReady", ns, podName)
	}
	desired, err := parseSlotSpec(slotsSpec)
	if err != nil {
		return paramErr("invalid slots %q: %v", slotsSpec, err)
	}
	if len(desired) == 0 {
		return paramErr("slots %q contains no slot numbers", slotsSpec)
	}
	if overlap := precedingAddSlotsOverlap(p, stepIndex, ns, podName, desired); overlap >= 0 {
		return paramErr("step %q: slot %d overlaps a previous AddSlots", p.Steps[stepIndex].ID, overlap)
	}

	pod := &corev1.Pod{}
	if err := e.Get(ctx, client.ObjectKey{Namespace: ns, Name: podName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			return paramErr("pod %s/%s does not exist", ns, podName)
		}
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get pod: %v", err)}, err
	}
	if pod.Status.PodIP == "" {
		return running("pod %s/%s has no IP yet", ns, podName), nil
	}

	addr := podRedisAddr(pod)
	rc, err := e.RedisFactory(addr)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for %s: %v", addr, err)}, err
	}
	defer rc.Close()

	if err := rc.Ping(ctx); err != nil {
		return running("redis at %s not reachable: %v", addr, err), nil
	}

	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		return running("redis at %s CLUSTER NODES failed: %v", addr, err), nil
	}
	entries := parseClusterNodes(nodesOut)
	target := findByIP(entries, pod.Status.PodIP)
	if target == nil {
		return running("target %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, podName, pod.Status.PodIP), nil
	}
	if !target.isMaster() {
		return paramErr("target %s/%s is not a master (flags=%v)", ns, podName, target.Flags)
	}
	if !target.healthy() {
		return paramErr("target master %s/%s is not healthy (flags=%v link=%q)", ns, podName, target.Flags, target.LinkState)
	}
	if !masterHasHealthyReplica(entries, target.ID) {
		return paramErr("master %s/%s has no healthy replica; AddSlots requires at least one", ns, podName)
	}

	migrating := migratingSlots(entries)
	owner, err := slotOwnership(entries)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse slot ownership: %v", err)}, err
	}

	var toAdd []int
	for _, s := range desired {
		if _, m := migrating[s]; m {
			return paramErr("slot %d is in migrating/importing state; resolve before AddSlots", s)
		}
		cur, owned := owner[s]
		switch {
		case !owned:
			toAdd = append(toAdd, s)
		case cur == target.ID:
			// already owned by target; nothing to do for this slot
		default:
			return paramErr("slot %d already owned by node %s, not target %s; use MigrateSlots instead", s, cur, target.ID)
		}
	}

	if len(toAdd) == 0 {
		refreshExistingTopologySlots(cluster, podName, target.ID, slotsSpec)
		return completed("slots %s already owned by master %s/%s", slotsSpec, ns, podName), nil
	}

	for i := 0; i < len(toAdd); i += addSlotsBatchSize {
		end := i + addSlotsBatchSize
		if end > len(toAdd) {
			end = len(toAdd)
		}
		if err := rc.ClusterAddSlots(ctx, toAdd[i:end]); err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CLUSTER ADDSLOTS batch %d-%d: %v", toAdd[i], toAdd[end-1], err)}, err
		}
	}

	nodesOut, err = rc.ClusterNodes(ctx)
	if err != nil {
		return running("redis at %s CLUSTER NODES after addslots failed: %v", addr, err), nil
	}
	entries = parseClusterNodes(nodesOut)
	target = findByIP(entries, pod.Status.PodIP)
	if target == nil {
		return running("target %s/%s disappeared after CLUSTER ADDSLOTS", ns, podName), nil
	}
	owner, err = slotOwnership(entries)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse slot ownership after addslots: %v", err)}, err
	}
	for _, s := range desired {
		if owner[s] != target.ID {
			return running("CLUSTER ADDSLOTS issued; slot %d not yet owned by target %s/%s", s, ns, podName), nil
		}
	}
	refreshExistingTopologySlots(cluster, podName, target.ID, slotsSpec)
	return completed("slots %s assigned to master %s/%s", slotsSpec, ns, podName), nil
}

// precedingAddSlotsOverlap returns the first slot in `desired` that is also
// claimed by an earlier AddSlots step in p (same namespace), or -1 if there
// is no overlap. This mirrors the Validator's overlap check but is enforced
// again at execution time to stay safe against stale plans.
func precedingAddSlotsOverlap(p *plan.Plan, stepIndex int, ns string, _ string, desired []int) int {
	covered := map[int]struct{}{}
	for i := 0; i < stepIndex; i++ {
		s := p.Steps[i]
		if s.Action != plan.ActionAddSlots {
			continue
		}
		ens, ok1 := paramString(s.Params, "namespace")
		if !ok1 || ens != ns {
			continue
		}
		prev, err := parseSlotSpec(stringOrEmpty(s.Params, "slots"))
		if err != nil {
			continue
		}
		for _, s2 := range prev {
			covered[s2] = struct{}{}
		}
	}
	for _, s := range desired {
		if _, c := covered[s]; c {
			return s
		}
	}
	return -1
}

// stringOrEmpty reads a string param, returning "" if missing.
func stringOrEmpty(params map[string]any, key string) string {
	v, ok := paramString(params, key)
	if !ok {
		return ""
	}
	return v
}

// masterHasHealthyReplica reports whether the master identified by masterID
// has at least one replica that is connected and not in a fail state. This is
// the runtime re-check of the Validator's "AddSlots requires a replica"
// precondition, since replicas can disappear or fail between plan time and
// execution time.
func masterHasHealthyReplica(entries []clusterNodeEntry, masterID string) bool {
	for _, e := range entries {
		if e.isReplica() && e.MasterID == masterID && e.healthy() {
			return true
		}
	}
	return false
}

// refreshExistingTopologyNode's slot-aware counterpart: refreshes the
// NodeID/Ready/Slots of any status.topology entry that already references
// podName. It does NOT create shards or nodes, consistent with the minimal
// topology approach used by the other action executors.
func refreshExistingTopologySlots(cluster *v1alpha1.RedisCluster, podName, nodeID, slots string) {
	if cluster.Status.Topology == nil {
		return
	}
	for i := range cluster.Status.Topology.Shards {
		sh := &cluster.Status.Topology.Shards[i]
		if sh.Master.Pod == podName {
			sh.Master.NodeID = nodeID
			sh.Master.Ready = true
			sh.Master.Slots = slots
		}
		for j := range sh.Replicas {
			if sh.Replicas[j].Pod == podName {
				sh.Replicas[j].NodeID = nodeID
				sh.Replicas[j].Ready = true
				sh.Replicas[j].Slots = slots
			}
		}
	}
}
