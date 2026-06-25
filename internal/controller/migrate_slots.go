package controller

import (
	"context"
	"fmt"
	"sync"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"
	"sigs.k8s.io/controller-runtime/pkg/log"

	v1alpha1 "github.com/Zumium/llm-k8s-redis/api/v1alpha1"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
	"github.com/Zumium/llm-k8s-redis/internal/redis"
	"github.com/Zumium/llm-k8s-redis/internal/rediscluster"
)

const (
	migrateSlotsPerReconcile = 8
	migrateKeysPerSlot       = 100
	migrateKeyTimeout        = 10 * time.Second
)

func (e *ActionExecutor) migrateSlots(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
	logger := log.FromContext(ctx)
	logger.Info("migrate slots started")
	ns, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "namespace")
	if !ok {
		return outcome, err
	}
	sourcePodName, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "sourcePod")
	if !ok {
		return outcome, err
	}
	targetPodName, outcome, err, ok := requireString(p.Steps[stepIndex].Params, "targetPod")
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
	if sourcePodName == targetPodName {
		return paramErr("sourcePod and targetPod must differ")
	}
	if !podDeclaredOrInTopology(cluster, p, stepIndex, ns, sourcePodName) {
		return paramErr("source pod %s/%s was not declared by a preceding EnsureNode and is not in topology", ns, sourcePodName)
	}
	if !podDeclaredOrInTopology(cluster, p, stepIndex, ns, targetPodName) {
		return paramErr("target pod %s/%s was not declared by a preceding EnsureNode and is not in topology", ns, targetPodName)
	}
	if !precededWaitNodeReady(p, stepIndex, ns, targetPodName) && !podInTopology(cluster, targetPodName) {
		return paramErr("target pod %s/%s has not completed a preceding WaitNodeReady", ns, targetPodName)
	}
	desired, err := rediscluster.ParseSlotSpec(slotsSpec)
	if err != nil {
		return paramErr("invalid slots %q: %v", slotsSpec, err)
	}
	if len(desired) == 0 {
		return paramErr("slots %q contains no slot numbers", slotsSpec)
	}
	logger.Info("migrate slots parsed", "namespace", ns, "sourcePod", sourcePodName, "targetPod", targetPodName, "slots", slotsSpec, "slotCount", len(desired))

	sourcePod, outcome, err, ok := e.getPod(ctx, ns, sourcePodName)
	if !ok {
		return outcome, err
	}
	targetPod, outcome, err, ok := e.getPod(ctx, ns, targetPodName)
	if !ok {
		return outcome, err
	}
	if sourcePod.Status.PodIP == "" {
		logger.Info("source pod has no IP for migrate slots", "namespace", ns, "pod", sourcePodName)
		return running("source pod %s/%s has no IP yet", ns, sourcePodName), nil
	}
	if targetPod.Status.PodIP == "" {
		logger.Info("target pod has no IP for migrate slots", "namespace", ns, "pod", targetPodName)
		return running("target pod %s/%s has no IP yet", ns, targetPodName), nil
	}

	if e.RedisFactory == nil {
		e.RedisFactory = redis.DefaultFactory
		logger.Info("default redis factory initialized")
	}
	logger.Info("building redis clients for migrate slots", "sourceAddr", podRedisAddr(sourcePod), "targetAddr", podRedisAddr(targetPod))
	sourceRC, err := e.RedisFactory(podRedisAddr(sourcePod))
	if err != nil {
		logger.Error(err, "build redis client for source migrate slots failed", "addr", podRedisAddr(sourcePod))
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for source %s: %v", podRedisAddr(sourcePod), err)}, err
	}
	defer sourceRC.Close()
	targetRC, err := e.RedisFactory(podRedisAddr(targetPod))
	if err != nil {
		logger.Error(err, "build redis client for target migrate slots failed", "addr", podRedisAddr(targetPod))
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for target %s: %v", podRedisAddr(targetPod), err)}, err
	}
	defer targetRC.Close()

	sourcePingStart := time.Now()
	if err := sourceRC.Ping(ctx); err != nil {
		logger.Info("source redis ping before migrate slots failed", "addr", podRedisAddr(sourcePod), "duration", time.Since(sourcePingStart), "error", err)
		return running("source redis at %s not reachable: %v", podRedisAddr(sourcePod), err), nil
	}
	logger.Info("source redis ping before migrate slots succeeded", "addr", podRedisAddr(sourcePod), "duration", time.Since(sourcePingStart))
	targetPingStart := time.Now()
	if err := targetRC.Ping(ctx); err != nil {
		logger.Info("target redis ping before migrate slots failed", "addr", podRedisAddr(targetPod), "duration", time.Since(targetPingStart), "error", err)
		return running("target redis at %s not reachable: %v", podRedisAddr(targetPod), err), nil
	}
	logger.Info("target redis ping before migrate slots succeeded", "addr", podRedisAddr(targetPod), "duration", time.Since(targetPingStart))

	nodesStart := time.Now()
	nodesOut, err := sourceRC.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes before migrate slots failed", "addr", podRedisAddr(sourcePod), "duration", time.Since(nodesStart), "error", err)
		return running("source redis at %s CLUSTER NODES failed: %v", podRedisAddr(sourcePod), err), nil
	}
	entries := rediscluster.ParseNodes(nodesOut)
	logger.Info("cluster nodes before migrate slots read", "addr", podRedisAddr(sourcePod), "duration", time.Since(nodesStart), "entries", len(entries))
	source := rediscluster.FindByIP(entries, sourcePod.Status.PodIP)
	if source == nil {
		logger.Info("source not visible before migrate slots", "sourcePod", sourcePodName, "sourceIP", sourcePod.Status.PodIP)
		return running("source %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, sourcePodName, sourcePod.Status.PodIP), nil
	}
	target := rediscluster.FindByIP(entries, targetPod.Status.PodIP)
	if target == nil {
		logger.Info("target not visible before migrate slots", "targetPod", targetPodName, "targetIP", targetPod.Status.PodIP)
		return running("target %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, targetPodName, targetPod.Status.PodIP), nil
	}
	if !source.IsMaster() || !source.Healthy() {
		logger.Info("source is not healthy master before migrate slots", "sourcePod", sourcePodName, "flags", source.Flags, "link", source.LinkState)
		return paramErr("source %s/%s is not a healthy master (flags=%v link=%q)", ns, sourcePodName, source.Flags, source.LinkState)
	}
	if !target.IsMaster() || !target.Healthy() {
		logger.Info("target is not healthy master before migrate slots", "targetPod", targetPodName, "flags", target.Flags, "link", target.LinkState)
		return paramErr("target %s/%s is not a healthy master (flags=%v link=%q)", ns, targetPodName, target.Flags, target.LinkState)
	}
	if !rediscluster.MasterHasHealthyReplica(entries, target.ID) {
		logger.Info("target master has no healthy replica before migrate slots", "targetPod", targetPodName, "targetID", target.ID)
		return running("target master %s/%s has no healthy replica yet; waiting before MigrateSlots", ns, targetPodName), nil
	}

	owner, err := rediscluster.SlotOwnership(entries)
	if err != nil {
		logger.Error(err, "parse slot ownership before migrate slots failed")
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse slot ownership: %v", err)}, err
	}
	markers := rediscluster.SlotMigrationMarkers(entries)
	for _, slot := range desired {
		if marker, ok := markers[slot]; ok && (marker.SourceID != source.ID || marker.TargetID != target.ID) {
			return paramErr("slot %d is migrating/importing between %s and %s, not requested %s->%s", slot, marker.SourceID, marker.TargetID, source.ID, target.ID)
		}
		cur, ok := owner[slot]
		if !ok {
			return paramErr("slot %d has no owner", slot)
		}
		if cur != source.ID && cur != target.ID {
			return paramErr("slot %d owned by node %s, not source %s or target %s", slot, cur, source.ID, target.ID)
		}
	}

	todo := []int{}
	for _, slot := range desired {
		if owner[slot] == target.ID {
			logger.Info("slot already owned by target", "slot", slot, "targetID", target.ID)
			continue
		}
		if len(todo) >= migrateSlotsPerReconcile {
			break
		}
		todo = append(todo, slot)
	}

	slotCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	var firstErr migrateSlotResult
	migratedKeys := 0
	completedSlots := 0
	var wg sync.WaitGroup
	for _, slot := range todo {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			result := e.migrateSlot(slotCtx, sourceRC, targetRC, source.ID, target.ID, target.Addr, slot)
			mu.Lock()
			if firstErr.err == nil && result.err != nil {
				firstErr = result
				cancel()
			}
			if result.keys > 0 {
				migratedKeys += result.keys
			} else if result.err == nil {
				completedSlots++
			}
			mu.Unlock()
		}(slot)
	}
	wg.Wait()

	if firstErr.err != nil {
		return firstErr.outcome, firstErr.err
	}
	if migratedKeys > 0 {
		return running("migrated %d keys across %d slots; waiting to finish slot ownership", migratedKeys, len(todo)-completedSlots), nil
	}

	afterStart := time.Now()
	nodesOut, err = sourceRC.ClusterNodes(ctx)
	if err != nil {
		logger.Info("cluster nodes after migrate slots failed", "addr", podRedisAddr(sourcePod), "duration", time.Since(afterStart), "error", err)
		return running("source redis at %s CLUSTER NODES after migration failed: %v", podRedisAddr(sourcePod), err), nil
	}
	entries = rediscluster.ParseNodes(nodesOut)
	logger.Info("cluster nodes after migrate slots read", "addr", podRedisAddr(sourcePod), "duration", time.Since(afterStart), "entries", len(entries))
	owner, err = rediscluster.SlotOwnership(entries)
	if err != nil {
		logger.Error(err, "parse slot ownership after migrate slots failed")
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse slot ownership after migration: %v", err)}, err
	}
	target = rediscluster.FindByIP(entries, targetPod.Status.PodIP)
	if target == nil {
		logger.Info("target disappeared after migrate slots", "targetPod", targetPodName)
		return running("target %s/%s disappeared after migration", ns, targetPodName), nil
	}
	for _, slot := range desired {
		if owner[slot] != target.ID {
			logger.Info("slot not yet owned by target after migrate slots", "slot", slot, "targetID", target.ID)
			return running("slot %d not yet owned by target %s/%s", slot, ns, targetPodName), nil
		}
	}
	logger.Info("migrate slots verified", "sourcePod", sourcePodName, "targetPod", targetPodName, "targetID", target.ID, "slotCount", len(desired))
	refreshExistingTopologySlots(cluster, targetPodName, target.ID, slotsSpec)
	return completed("slots %s migrated from %s/%s to %s/%s", slotsSpec, ns, sourcePodName, ns, targetPodName), nil
}

type migrateSlotResult struct {
	outcome StepOutcome
	err     error
	keys    int
}

func (e *ActionExecutor) migrateSlot(ctx context.Context, sourceRC, targetRC redis.Client, sourceID, targetID, targetAddr string, slot int) migrateSlotResult {
	logger := log.FromContext(ctx)
	slotStart := time.Now()
	logger.Info("migrating slot started", "slot", slot, "sourceID", sourceID, "targetID", targetID)
	if err := targetRC.ClusterSetSlotImporting(ctx, slot, sourceID); err != nil {
		logger.Error(err, "target setslot importing failed", "slot", slot, "duration", time.Since(slotStart))
		return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("target SETSLOT IMPORTING slot %d: %v", slot, err)}, err: err}
	}
	if err := sourceRC.ClusterSetSlotMigrating(ctx, slot, targetID); err != nil {
		logger.Error(err, "source setslot migrating failed", "slot", slot, "duration", time.Since(slotStart))
		return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("source SETSLOT MIGRATING slot %d: %v", slot, err)}, err: err}
	}
	keys, err := sourceRC.ClusterGetKeysInSlot(ctx, slot, migrateKeysPerSlot)
	if err != nil {
		logger.Error(err, "get keys in slot failed", "slot", slot, "duration", time.Since(slotStart))
		return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("GETKEYSINSLOT slot %d: %v", slot, err)}, err: err}
	}
	logger.Info("keys in slot read", "slot", slot, "keys", len(keys), "limit", migrateKeysPerSlot)
	if len(keys) > 0 {
		host, port := rediscluster.RedisHostPortFromAddr(targetAddr)
		migrateStart := time.Now()
		logger.Info("migrating keys started", "slot", slot, "keys", len(keys), "host", host, "port", port)
		if err := sourceRC.MigrateKeys(ctx, host, port, keys, migrateKeyTimeout); err != nil {
			logger.Error(err, "migrate keys failed", "slot", slot, "keys", len(keys), "duration", time.Since(migrateStart))
			return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("MIGRATE slot %d keys: %v", slot, err)}, err: err}
		}
		logger.Info("migrating keys finished", "slot", slot, "keys", len(keys), "duration", time.Since(migrateStart))
		return migrateSlotResult{keys: len(keys)}
	}
	if err := sourceRC.ClusterSetSlotNode(ctx, slot, targetID); err != nil {
		logger.Error(err, "source setslot node failed", "slot", slot, "duration", time.Since(slotStart))
		return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("source SETSLOT NODE slot %d: %v", slot, err)}, err: err}
	}
	if err := targetRC.ClusterSetSlotNode(ctx, slot, targetID); err != nil {
		logger.Error(err, "target setslot node failed", "slot", slot, "duration", time.Since(slotStart))
		return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("target SETSLOT NODE slot %d: %v", slot, err)}, err: err}
	}
	logger.Info("migrating slot finished", "slot", slot, "duration", time.Since(slotStart))
	return migrateSlotResult{}
}

func (e *ActionExecutor) getPod(ctx context.Context, ns, podName string) (*corev1.Pod, StepOutcome, error, bool) {
	start := time.Now()
	logger := log.FromContext(ctx).WithValues("namespace", ns, "pod", podName)
	logger.Info("getting pod")
	pod := &corev1.Pod{}
	if err := e.Get(ctx, client.ObjectKey{Namespace: ns, Name: podName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			logger.Info("pod not found", "duration", time.Since(start))
			o, e := paramErr("pod %s/%s does not exist", ns, podName)
			return nil, o, e, false
		}
		logger.Error(err, "get pod failed", "duration", time.Since(start))
		return nil, StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get pod: %v", err)}, err, false
	}
	logger.Info("pod found", "duration", time.Since(start), "ready", podReady(pod), "ip", pod.Status.PodIP)
	return pod, StepOutcome{}, nil, true
}

func podDeclaredOrInTopology(cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int, ns, podName string) bool {
	return precededEnsureNode(p, stepIndex, ns, podName) || podInTopology(cluster, podName)
}
