package action

import (
	"context"
	"fmt"
	"sort"
	"strings"
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
	if outcome, err, ok := validateClusterNamespace(cluster, ns); !ok {
		return outcome, err
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
	if !podWaitedOrInTopology(cluster, p, stepIndex, ns, targetPodName) {
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
	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		return outcome, fmt.Errorf("%s", outcome.Message)
	}

	owner, err := rediscluster.SlotOwnership(entries)
	if err != nil {
		logger.Error(err, "parse slot ownership before migrate slots failed")
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse slot ownership: %v", err)}, err
	}
	podsByIP := mapPodsByIP(pods)
	entriesByID := map[string]rediscluster.Entry{}
	for _, entry := range entries {
		entriesByID[entry.ID] = entry
	}
	markers := rediscluster.SlotMigrationMarkers(entries)
	missing := []int{}
	sources := map[int]corev1.Pod{}
	for _, slot := range desired {
		cur, ok := owner[slot]
		if !ok {
			if marker, ok := markers[slot]; ok && marker.TargetID != target.ID {
				return paramErr("slot %d is migrating/importing between %s and %s, not requested target %s", slot, marker.SourceID, marker.TargetID, target.ID)
			}
			missing = append(missing, slot)
			continue
		}
		if marker, ok := markers[slot]; ok && (marker.SourceID != cur || marker.TargetID != target.ID) {
			return paramErr("slot %d is migrating/importing between %s and %s, not requested %s->%s", slot, marker.SourceID, marker.TargetID, cur, target.ID)
		}
		if cur == target.ID {
			continue
		}
		sourceEntry := entriesByID[cur]
		sourceIP := rediscluster.IPFromAddr(rediscluster.RedisAddrFromClusterAddr(sourceEntry.Addr))
		sourcePod := podsByIP[sourceIP]
		if sourcePod == nil || !sourceEntry.IsMaster() || !sourceEntry.Healthy() {
			return paramErr("slot %d owned by unmanaged or unhealthy node %s, not source %s or target %s", slot, cur, source.ID, target.ID)
		}
		sources[slot] = *sourcePod
	}
	if len(missing) > 0 {
		targetNodesOut, err := targetRC.ClusterNodes(ctx)
		if err != nil {
			logger.Info("target cluster nodes before missing slot repair failed", "addr", podRedisAddr(targetPod), "error", err)
			return running("target redis at %s CLUSTER NODES before missing slot repair failed: %v", podRedisAddr(targetPod), err), nil
		}
		targetOwner, err := rediscluster.SlotOwnership(rediscluster.ParseNodes(targetNodesOut))
		if err != nil {
			logger.Error(err, "parse target slot ownership before missing slot repair failed")
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse target slot ownership: %v", err)}, err
		}
		toAdd := []int{}
		for _, slot := range missing {
			cur, ok := targetOwner[slot]
			switch {
			case !ok:
				toAdd = append(toAdd, slot)
			case cur == target.ID:
				owner[slot] = target.ID
			case cur == source.ID:
				return running("slot %d ownership is not yet consistent between source and target", slot), nil
			default:
				return paramErr("slot %d owned by node %s, not source %s or target %s", slot, cur, source.ID, target.ID)
			}
		}
		if len(toAdd) > 0 {
			if outcome, ok := waitForMigrationClusterStateOK(ctx, podRedisAddr(targetPod), targetRC, nil, nil); !ok {
				return outcome, nil
			}
			logger.Info("repairing unowned slots before migrate", "targetPod", targetPodName, "targetID", target.ID, "slotCount", len(toAdd))
			if err := targetRC.ClusterAddSlots(ctx, toAdd); err != nil {
				logger.Error(err, "cluster addslots for unowned migrate slots failed", "targetPod", targetPodName, "slotCount", len(toAdd))
				if transientRedisPropagationError(err) {
					return running("CLUSTER ADDSLOTS missing migrate slots waiting for Redis cluster state: %v", err), nil
				}
				return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("CLUSTER ADDSLOTS missing migrate slots: %v", err)}, err
			}
			for _, slot := range toAdd {
				owner[slot] = target.ID
			}
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
	if len(todo) > 0 {
		if outcome, ok := e.waitForMigratePeerViews(ctx, pods, entries, targetPod.Status.PodIP, target.ID); !ok {
			return outcome, nil
		}
	}

	sourceClients := map[string]redis.Client{}
	defer func() {
		for _, rc := range sourceClients {
			_ = rc.Close()
		}
	}()
	for _, slot := range todo {
		pod := sources[slot]
		addr := podRedisAddr(&pod)
		if _, ok := sourceClients[addr]; ok {
			continue
		}
		rc, err := e.RedisFactory(addr)
		if err != nil {
			logger.Error(err, "build redis client for live slot owner failed", "addr", addr)
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for live slot owner %s: %v", addr, err)}, err
		}
		sourceClients[addr] = rc
	}
	extraNodeClients := map[string]redis.Client{}
	defer func() {
		for _, rc := range extraNodeClients {
			_ = rc.Close()
		}
	}()
	targetAddr := podRedisAddr(targetPod)
	for _, entry := range entries {
		if !entry.IsMaster() || !entry.Healthy() {
			continue
		}
		addr := rediscluster.RedisAddrFromClusterAddr(entry.Addr)
		if addr == targetAddr || sourceClients[addr] != nil {
			continue
		}
		ip := rediscluster.IPFromAddr(addr)
		if podsByIP[ip] == nil {
			continue
		}
		rc, err := e.RedisFactory(addr)
		if err != nil {
			logger.Error(err, "build redis client for migrate slot owner propagation failed", "addr", addr)
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for slot owner propagation %s: %v", addr, err)}, err
		}
		extraNodeClients[addr] = rc
	}
	if outcome, ok := waitForMigrationClusterStateOK(ctx, targetAddr, targetRC, sourceClients, extraNodeClients); !ok {
		return outcome, nil
	}

	slotCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var mu sync.Mutex
	var firstErr migrateSlotResult
	var firstWait migrateSlotResult
	migratedKeys := 0
	completedSlots := 0
	var wg sync.WaitGroup
	for _, slot := range todo {
		wg.Add(1)
		go func(slot int) {
			defer wg.Done()
			pod := sources[slot]
			sourceEntry := entriesByID[owner[slot]]
			result := e.migrateSlot(slotCtx, sourceClients[podRedisAddr(&pod)], targetRC, extraNodeClients, sourceEntry.ID, target.ID, target.Addr, slot)
			mu.Lock()
			if firstErr.err == nil && result.err != nil {
				firstErr = result
				cancel()
			}
			if firstErr.err == nil && !firstWait.wait && result.wait {
				firstWait = result
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
	if firstWait.wait {
		return firstWait.outcome, nil
	}
	if migratedKeys > 0 {
		return running("migrated %d keys across %d slots; waiting to finish slot ownership", migratedKeys, len(todo)-completedSlots), nil
	}

	outcome, err, ok = e.verifyMigratedSlots(ctx, cluster, entries, sourcePodName, targetPodName, target.ID, desired)
	if !ok {
		return outcome, err
	}
	logger.Info("migrate slots verified", "sourcePod", sourcePodName, "targetPod", targetPodName, "targetID", target.ID, "slotCount", len(desired))
	refreshExistingTopologySlots(cluster, targetPodName, target.ID, slotsSpec)
	return completed("slots %s migrated from %s/%s to %s/%s", slotsSpec, ns, sourcePodName, ns, targetPodName), nil
}

type migrateSlotResult struct {
	outcome StepOutcome
	err     error
	keys    int
	wait    bool
}

func (e *ActionExecutor) verifyMigratedSlots(ctx context.Context, cluster *v1alpha1.RedisCluster, entries []rediscluster.Entry, sourcePodName, targetPodName, targetID string, desired []int) (StepOutcome, error, bool) {
	pods, outcome, ok := e.listManagedPods(ctx, cluster)
	if !ok {
		return outcome, fmt.Errorf("%s", outcome.Message), false
	}
	candidates := migrateVerifyPods(cluster, pods, entries, sourcePodName, targetPodName)
	if len(candidates) == 0 {
		return running("no healthy master available to verify migrated slots"), nil, false
	}
	if len(candidates) > 3 {
		candidates = candidates[:3]
	}

	logger := log.FromContext(ctx)
	for _, pod := range candidates {
		addr := podRedisAddr(&pod)
		start := time.Now()
		rc, err := e.RedisFactory(addr)
		if err != nil {
			logger.Info("build redis client for migrate slot verification failed", "addr", addr, "error", err)
			return running("master %s CLUSTER NODES verification not ready: %v", addr, err), nil, false
		}
		nodesOut, err := rc.ClusterNodes(ctx)
		closeErr := rc.Close()
		if err != nil {
			logger.Info("cluster nodes verification after migrate slots failed", "addr", addr, "duration", time.Since(start), "error", err)
			return running("master %s CLUSTER NODES after migration failed: %v", addr, err), nil, false
		}
		if closeErr != nil {
			logger.Info("close redis client after migrate slot verification failed", "addr", addr, "error", closeErr)
		}
		owner, err := rediscluster.SlotOwnership(rediscluster.ParseNodes(nodesOut))
		if err != nil {
			logger.Info("parse slot ownership during migrate verification failed", "addr", addr, "error", err)
			return running("master %s slot ownership not yet consistent: %v", addr, err), nil, false
		}
		for _, slot := range desired {
			if owner[slot] != targetID {
				logger.Info("slot not yet owned by target in master view", "addr", addr, "slot", slot, "targetID", targetID, "ownerID", owner[slot])
				return running("slot %d not yet owned by target in master %s view", slot, addr), nil, false
			}
		}
	}
	logger.Info("migrate slots verified from master views", "masters", len(candidates), "targetID", targetID, "slotCount", len(desired))
	return StepOutcome{}, nil, true
}

func migrateVerifyPods(cluster *v1alpha1.RedisCluster, pods []corev1.Pod, entries []rediscluster.Entry, sourcePodName, targetPodName string) []corev1.Pod {
	podsByName := map[string]corev1.Pod{}
	for _, pod := range pods {
		podsByName[pod.Name] = pod
	}
	out := []corev1.Pod{}
	seen := map[string]bool{}
	add := func(name string) {
		pod, ok := podsByName[name]
		if !ok || seen[pod.Name] || !podReady(&pod) || pod.Status.PodIP == "" {
			return
		}
		entry := rediscluster.FindByIP(entries, pod.Status.PodIP)
		if entry == nil || !entry.IsMaster() || !entry.Healthy() {
			return
		}
		seen[pod.Name] = true
		out = append(out, pod)
	}
	add(sourcePodName)
	add(targetPodName)
	for _, pod := range observationSeedPods(cluster, pods) {
		add(pod.Name)
	}
	return out
}

func (e *ActionExecutor) waitForMigratePeerViews(ctx context.Context, pods []corev1.Pod, entries []rediscluster.Entry, targetIP, targetID string) (StepOutcome, bool) {
	podsByIP := mapPodsByIP(pods)
	for _, entry := range entries {
		addr := rediscluster.RedisAddrFromClusterAddr(entry.Addr)
		ip := rediscluster.IPFromAddr(addr)
		pod := podsByIP[ip]
		if pod == nil {
			continue
		}
		if !podReady(pod) || pod.Status.PodIP == "" {
			continue
		}
		rc, err := e.RedisFactory(addr)
		if err != nil {
			return running("peer %s not ready for migration preflight: %v", addr, err), false
		}
		raw, err := rc.ClusterNodes(ctx)
		closeErr := rc.Close()
		if err != nil {
			return running("peer %s CLUSTER NODES not ready for migration preflight: %v", addr, err), false
		}
		if closeErr != nil {
			log.FromContext(ctx).Info("close redis client after migrate preflight failed", "addr", addr, "error", closeErr)
		}
		entries := rediscluster.ParseNodes(raw)
		target := rediscluster.FindByIP(entries, targetIP)
		if target == nil || target.ID != targetID || !target.IsMaster() || !target.Healthy() {
			return running("peer %s does not yet see target %s as healthy master", addr, targetID), false
		}
		if !rediscluster.MasterHasHealthyReplica(entries, targetID) {
			return running("peer %s does not yet see target %s with healthy replica", addr, targetID), false
		}
	}
	return StepOutcome{}, true
}

func (e *ActionExecutor) migrateSlot(ctx context.Context, sourceRC, targetRC redis.Client, extraNodeClients map[string]redis.Client, sourceID, targetID, targetAddr string, slot int) migrateSlotResult {
	logger := log.FromContext(ctx)
	slotStart := time.Now()
	logger.Info("migrating slot started", "slot", slot, "sourceID", sourceID, "targetID", targetID)
	if err := targetRC.ClusterSetSlotImporting(ctx, slot, sourceID); err != nil {
		logger.Error(err, "target setslot importing failed", "slot", slot, "duration", time.Since(slotStart))
		if transientRedisPropagationError(err) {
			return migrateSlotResult{outcome: running("target SETSLOT IMPORTING slot %d waiting for Redis gossip: %v", slot, err), wait: true}
		}
		return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("target SETSLOT IMPORTING slot %d: %v", slot, err)}, err: err}
	}
	if err := sourceRC.ClusterSetSlotMigrating(ctx, slot, targetID); err != nil {
		logger.Error(err, "source setslot migrating failed", "slot", slot, "duration", time.Since(slotStart))
		if transientRedisPropagationError(err) {
			return migrateSlotResult{outcome: running("source SETSLOT MIGRATING slot %d waiting for Redis gossip: %v", slot, err), wait: true}
		}
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
			if transientRedisPropagationError(err) {
				return migrateSlotResult{outcome: running("MIGRATE slot %d waiting for Redis cluster state: %v", slot, err), wait: true}
			}
			return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("MIGRATE slot %d keys: %v", slot, err)}, err: err}
		}
		logger.Info("migrating keys finished", "slot", slot, "keys", len(keys), "duration", time.Since(migrateStart))
		return migrateSlotResult{keys: len(keys)}
	}
	if err := sourceRC.ClusterSetSlotNode(ctx, slot, targetID); err != nil {
		logger.Error(err, "source setslot node failed", "slot", slot, "duration", time.Since(slotStart))
		if transientRedisPropagationError(err) {
			return migrateSlotResult{outcome: running("source SETSLOT NODE slot %d waiting for Redis gossip: %v", slot, err), wait: true}
		}
		return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("source SETSLOT NODE slot %d: %v", slot, err)}, err: err}
	}
	if err := targetRC.ClusterSetSlotNode(ctx, slot, targetID); err != nil {
		logger.Error(err, "target setslot node failed", "slot", slot, "duration", time.Since(slotStart))
		if transientRedisPropagationError(err) {
			return migrateSlotResult{outcome: running("target SETSLOT NODE slot %d waiting for Redis gossip: %v", slot, err), wait: true}
		}
		return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("target SETSLOT NODE slot %d: %v", slot, err)}, err: err}
	}
	addrs := make([]string, 0, len(extraNodeClients))
	for addr := range extraNodeClients {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)
	for _, addr := range addrs {
		rc := extraNodeClients[addr]
		if err := rc.ClusterSetSlotNode(ctx, slot, targetID); err != nil {
			logger.Error(err, "peer setslot node failed", "addr", addr, "slot", slot, "duration", time.Since(slotStart))
			if transientRedisPropagationError(err) {
				return migrateSlotResult{outcome: running("peer %s SETSLOT NODE slot %d waiting for Redis gossip: %v", addr, slot, err), wait: true}
			}
			return migrateSlotResult{outcome: StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("peer %s SETSLOT NODE slot %d: %v", addr, slot, err)}, err: err}
		}
	}
	logger.Info("migrating slot finished", "slot", slot, "duration", time.Since(slotStart))
	return migrateSlotResult{}
}

func waitForMigrationClusterStateOK(ctx context.Context, targetAddr string, targetRC redis.Client, sourceClients, extraNodeClients map[string]redis.Client) (StepOutcome, bool) {
	clients := map[string]redis.Client{targetAddr: targetRC}
	for addr, rc := range sourceClients {
		clients[addr] = rc
	}
	for addr, rc := range extraNodeClients {
		clients[addr] = rc
	}
	addrs := make([]string, 0, len(clients))
	for addr := range clients {
		addrs = append(addrs, addr)
	}
	sort.Strings(addrs)
	for _, addr := range addrs {
		raw, err := clients[addr].ClusterInfo(ctx)
		if err != nil {
			return running("redis %s CLUSTER INFO before migration failed: %v", addr, err), false
		}
		info := rediscluster.ParseInfo(raw)
		if !rediscluster.StateOK(info) {
			return running("redis %s cluster_state is %q before migration", addr, info["cluster_state"]), false
		}
	}
	return StepOutcome{}, true
}

func transientRedisPropagationError(err error) bool {
	if err == nil {
		return false
	}
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "please use setslot only with masters") ||
		strings.Contains(msg, "unknown node") ||
		strings.Contains(msg, "not known") ||
		strings.Contains(msg, "no such node") ||
		strings.Contains(msg, "clusterdown")
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
