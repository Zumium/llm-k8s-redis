package controller

import (
	"context"
	"fmt"
	"time"

	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
)

const addSlotsBatchSize = 512

func (e *ActionExecutor) addSlots(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	logger.Info("add slots started")
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

	if outcome, err, ok := validateClusterNamespace(cluster, ns); !ok {
		return outcome, err
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
	logger.Info("add slots parsed", "namespace", ns, "pod", podName, "slots", slotsSpec, "slotCount", len(desired))
	if overlap := precedingAddSlotsOverlap(p, stepIndex, ns, podName, desired); overlap >= 0 {
		return paramErr("step %q: slot %d overlaps a previous AddSlots", p.Steps[stepIndex].ID, overlap)
	}

	pod, outcome, err, ok := e.getPod(ctx, ns, podName)
	if !ok {
		return outcome, err
	}
	if pod.Status.PodIP == "" {
		logger.Info("target pod has no IP for add slots", "namespace", ns, "pod", podName)
		return running("pod %s/%s has no IP yet", ns, podName), nil
	}

	addr := podRedisAddr(pod)
	logger.Info("checking add slots target", "namespace", ns, "pod", podName, "addr", addr)
	rc, err := e.RedisFactory(addr)
	if err != nil {
		logger.Error(err, "build redis client for add slots failed", "addr", addr)
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for %s: %v", addr, err)}, err
	}
	defer rc.Close()

	pingStart := time.Now()
	if err := rc.Ping(ctx); err != nil {
		logger.Info("redis ping before add slots failed", "addr", addr, "duration", time.Since(pingStart), "error", err)
		return running("redis at %s not reachable: %v", addr, err), nil
	}
	logger.Info("redis ping before add slots succeeded", "addr", addr, "duration", time.Since(pingStart))

	nodesStart := time.Now()
	nodesOut, err := rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes before add slots failed", "addr", addr, "duration", time.Since(nodesStart), "error", err)
		return running("redis at %s CLUSTER NODES failed: %v", addr, err), nil
	}
	entries := rediscluster.ParseNodes(nodesOut)
	logger.Info("cluster nodes before add slots read", "addr", addr, "duration", time.Since(nodesStart), "entries", len(entries))
	target := rediscluster.FindByIP(entries, pod.Status.PodIP)
	if target == nil {
		logger.Info("target not visible before add slots", "pod", podName, "ip", pod.Status.PodIP)
		return running("target %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, podName, pod.Status.PodIP), nil
	}
	if !target.IsMaster() {
		logger.Info("target is not master before add slots", "pod", podName, "flags", target.Flags)
		return paramErr("target %s/%s is not a master (flags=%v)", ns, podName, target.Flags)
	}
	if !target.Healthy() {
		logger.Info("target master not healthy before add slots", "pod", podName, "flags", target.Flags, "link", target.LinkState)
		return paramErr("target master %s/%s is not healthy (flags=%v link=%q)", ns, podName, target.Flags, target.LinkState)
	}
	if !rediscluster.MasterHasHealthyReplica(entries, target.ID) {
		logger.Info("target master has no healthy replica before add slots", "pod", podName, "targetID", target.ID)
		return running("master %s/%s has no healthy replica yet; waiting before AddSlots", ns, podName), nil
	}

	migrating := rediscluster.MigratingSlots(entries)
	owner, err := rediscluster.SlotOwnership(entries)
	if err != nil {
		logger.Error(err, "parse slot ownership before add slots failed")
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
		logger.Info("all desired slots already owned by target", "pod", podName, "targetID", target.ID, "slotCount", len(desired))
		refreshExistingTopologySlots(cluster, podName, target.ID, slotsSpec)
		return completed("slots %s already owned by master %s/%s", slotsSpec, ns, podName), nil
	}

	logger.Info("adding missing slots", "pod", podName, "targetID", target.ID, "slotCount", len(toAdd), "batchSize", addSlotsBatchSize)
	for i := 0; i < len(toAdd); i += addSlotsBatchSize {
		end := i + addSlotsBatchSize
		if end > len(toAdd) {
			end = len(toAdd)
		}
		batchStart := time.Now()
		logger.Info("cluster addslots batch started", "firstSlot", toAdd[i], "lastSlot", toAdd[end-1], "count", end-i)
		if err := rc.ClusterAddSlots(ctx, toAdd[i:end]); err != nil {
			logger.Error(err, "cluster addslots batch failed", "duration", time.Since(batchStart), "firstSlot", toAdd[i], "lastSlot", toAdd[end-1])
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CLUSTER ADDSLOTS batch %d-%d: %v", toAdd[i], toAdd[end-1], err)}, err
		}
		logger.Info("cluster addslots batch finished", "duration", time.Since(batchStart), "firstSlot", toAdd[i], "lastSlot", toAdd[end-1], "count", end-i)
	}

	afterStart := time.Now()
	nodesOut, err = rc.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes after add slots failed", "addr", addr, "duration", time.Since(afterStart), "error", err)
		return running("redis at %s CLUSTER NODES after addslots failed: %v", addr, err), nil
	}
	entries = rediscluster.ParseNodes(nodesOut)
	logger.Info("cluster nodes after add slots read", "addr", addr, "duration", time.Since(afterStart), "entries", len(entries))
	target = rediscluster.FindByIP(entries, pod.Status.PodIP)
	if target == nil {
		logger.Info("target disappeared after add slots", "pod", podName)
		return running("target %s/%s disappeared after CLUSTER ADDSLOTS", ns, podName), nil
	}
	owner, err = rediscluster.SlotOwnership(entries)
	if err != nil {
		logger.Error(err, "parse slot ownership after add slots failed")
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse slot ownership after addslots: %v", err)}, err
	}
	for _, s := range desired {
		if owner[s] != target.ID {
			logger.Info("slot not yet owned by target after add slots", "slot", s, "targetID", target.ID)
			return running("CLUSTER ADDSLOTS issued; slot %d not yet owned by target %s/%s", s, ns, podName), nil
		}
	}
	logger.Info("add slots verified", "pod", podName, "targetID", target.ID, "slotCount", len(desired))
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
