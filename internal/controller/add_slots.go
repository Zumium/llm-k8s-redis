package controller

import (
	"context"
	"fmt"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/rediscluster"
)

const addSlotsBatchSize = 512

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
	desired, err := rediscluster.ParseSlotSpec(slotsSpec)
	if err != nil {
		return paramErr("invalid slots %q: %v", slotsSpec, err)
	}
	if len(desired) == 0 {
		return paramErr("slots %q contains no slot numbers", slotsSpec)
	}
	if overlap := precedingAddSlotsOverlap(p, stepIndex, ns, podName, desired); overlap >= 0 {
		return paramErr("step %q: slot %d overlaps a previous AddSlots", p.Steps[stepIndex].ID, overlap)
	}

	pod, outcome, err, ok := e.getPod(ctx, ns, podName)
	if !ok {
		return outcome, err
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
	entries := rediscluster.ParseNodes(nodesOut)
	target := rediscluster.FindByIP(entries, pod.Status.PodIP)
	if target == nil {
		return running("target %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, podName, pod.Status.PodIP), nil
	}
	if !target.IsMaster() {
		return paramErr("target %s/%s is not a master (flags=%v)", ns, podName, target.Flags)
	}
	if !target.Healthy() {
		return paramErr("target master %s/%s is not healthy (flags=%v link=%q)", ns, podName, target.Flags, target.LinkState)
	}
	if !rediscluster.MasterHasHealthyReplica(entries, target.ID) {
		return running("master %s/%s has no healthy replica yet; waiting before AddSlots", ns, podName), nil
	}

	migrating := rediscluster.MigratingSlots(entries)
	owner, err := rediscluster.SlotOwnership(entries)
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
	entries = rediscluster.ParseNodes(nodesOut)
	target = rediscluster.FindByIP(entries, pod.Status.PodIP)
	if target == nil {
		return running("target %s/%s disappeared after CLUSTER ADDSLOTS", ns, podName), nil
	}
	owner, err = rediscluster.SlotOwnership(entries)
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
		prev, err := rediscluster.ParseSlotSpec(stringOrEmpty(s.Params, "slots"))
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

func stringOrEmpty(params map[string]any, key string) string {
	v, ok := paramString(params, key)
	if !ok {
		return ""
	}
	return v
}

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
