package controller

import (
	"context"
	"fmt"
	"time"

	corev1 "k8s.io/api/core/v1"
	apierrors "k8s.io/apimachinery/pkg/api/errors"
	"sigs.k8s.io/controller-runtime/pkg/client"

	v1alpha1 "github.com/example/llm-k8s-redis/api/v1alpha1"
	"github.com/example/llm-k8s-redis/internal/plan"
	"github.com/example/llm-k8s-redis/internal/redis"
	"github.com/example/llm-k8s-redis/internal/rediscluster"
)

const (
	migrateSlotsPerReconcile = 8
	migrateKeysPerSlot       = 100
	migrateKeyTimeout        = 10 * time.Second
)

func (e *ActionExecutor) migrateSlots(ctx context.Context, cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int) (StepOutcome, error) {
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

	sourcePod, outcome, err, ok := e.getPod(ctx, ns, sourcePodName)
	if !ok {
		return outcome, err
	}
	targetPod, outcome, err, ok := e.getPod(ctx, ns, targetPodName)
	if !ok {
		return outcome, err
	}
	if sourcePod.Status.PodIP == "" {
		return running("source pod %s/%s has no IP yet", ns, sourcePodName), nil
	}
	if targetPod.Status.PodIP == "" {
		return running("target pod %s/%s has no IP yet", ns, targetPodName), nil
	}

	if e.RedisFactory == nil {
		e.RedisFactory = redis.DefaultFactory
	}
	sourceRC, err := e.RedisFactory(podRedisAddr(sourcePod))
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for source %s: %v", podRedisAddr(sourcePod), err)}, err
	}
	defer sourceRC.Close()
	targetRC, err := e.RedisFactory(podRedisAddr(targetPod))
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("build redis client for target %s: %v", podRedisAddr(targetPod), err)}, err
	}
	defer targetRC.Close()

	if err := sourceRC.Ping(ctx); err != nil {
		return running("source redis at %s not reachable: %v", podRedisAddr(sourcePod), err), nil
	}
	if err := targetRC.Ping(ctx); err != nil {
		return running("target redis at %s not reachable: %v", podRedisAddr(targetPod), err), nil
	}

	nodesOut, err := sourceRC.ClusterNodes(ctx)
	if err != nil {
		return running("source redis at %s CLUSTER NODES failed: %v", podRedisAddr(sourcePod), err), nil
	}
	entries := rediscluster.ParseNodes(nodesOut)
	source := rediscluster.FindByIP(entries, sourcePod.Status.PodIP)
	if source == nil {
		return running("source %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, sourcePodName, sourcePod.Status.PodIP), nil
	}
	target := rediscluster.FindByIP(entries, targetPod.Status.PodIP)
	if target == nil {
		return running("target %s/%s (ip %s) not yet visible in CLUSTER NODES", ns, targetPodName, targetPod.Status.PodIP), nil
	}
	if !source.IsMaster() || !source.Healthy() {
		return paramErr("source %s/%s is not a healthy master (flags=%v link=%q)", ns, sourcePodName, source.Flags, source.LinkState)
	}
	if !target.IsMaster() || !target.Healthy() {
		return paramErr("target %s/%s is not a healthy master (flags=%v link=%q)", ns, targetPodName, target.Flags, target.LinkState)
	}
	if !rediscluster.MasterHasHealthyReplica(entries, target.ID) {
		return running("target master %s/%s has no healthy replica yet; waiting before MigrateSlots", ns, targetPodName), nil
	}

	owner, err := rediscluster.SlotOwnership(entries)
	if err != nil {
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

	done := 0
	for _, slot := range desired {
		if owner[slot] == target.ID {
			continue
		}
		if done >= migrateSlotsPerReconcile {
			return running("migrated %d slots this reconcile; continuing slots %s", done, slotsSpec), nil
		}
		if err := targetRC.ClusterSetSlotImporting(ctx, slot, source.ID); err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("target SETSLOT IMPORTING slot %d: %v", slot, err)}, err
		}
		if err := sourceRC.ClusterSetSlotMigrating(ctx, slot, target.ID); err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("source SETSLOT MIGRATING slot %d: %v", slot, err)}, err
		}
		keys, err := sourceRC.ClusterGetKeysInSlot(ctx, slot, migrateKeysPerSlot)
		if err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("GETKEYSINSLOT slot %d: %v", slot, err)}, err
		}
		if len(keys) > 0 {
			host, port := rediscluster.RedisHostPortFromAddr(target.Addr)
			if err := sourceRC.MigrateKeys(ctx, host, port, keys, migrateKeyTimeout); err != nil {
				return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("MIGRATE slot %d keys: %v", slot, err)}, err
			}
			return running("migrated %d keys for slot %d; waiting to finish slot ownership", len(keys), slot), nil
		}
		if err := setSlotNodeOnHealthyMasters(ctx, e.RedisFactory, entries, slot, target.ID); err != nil {
			return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("SETSLOT NODE slot %d: %v", slot, err)}, err
		}
		done++
	}

	nodesOut, err = sourceRC.ClusterNodes(ctx)
	if err != nil {
		return running("source redis at %s CLUSTER NODES after migration failed: %v", podRedisAddr(sourcePod), err), nil
	}
	entries = rediscluster.ParseNodes(nodesOut)
	owner, err = rediscluster.SlotOwnership(entries)
	if err != nil {
		return StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("parse slot ownership after migration: %v", err)}, err
	}
	target = rediscluster.FindByIP(entries, targetPod.Status.PodIP)
	if target == nil {
		return running("target %s/%s disappeared after migration", ns, targetPodName), nil
	}
	for _, slot := range desired {
		if owner[slot] != target.ID {
			return running("slot %d not yet owned by target %s/%s", slot, ns, targetPodName), nil
		}
	}
	refreshExistingTopologySlots(cluster, targetPodName, target.ID, slotsSpec)
	return completed("slots %s migrated from %s/%s to %s/%s", slotsSpec, ns, sourcePodName, ns, targetPodName), nil
}

func (e *ActionExecutor) getPod(ctx context.Context, ns, podName string) (*corev1.Pod, StepOutcome, error, bool) {
	pod := &corev1.Pod{}
	if err := e.Get(ctx, client.ObjectKey{Namespace: ns, Name: podName}, pod); err != nil {
		if apierrors.IsNotFound(err) {
			o, e := paramErr("pod %s/%s does not exist", ns, podName)
			return nil, o, e, false
		}
		return nil, StepOutcome{Status: plan.StepStateFailed, Message: fmt.Sprintf("get pod: %v", err)}, err, false
	}
	return pod, StepOutcome{}, nil, true
}

func podDeclaredOrInTopology(cluster *v1alpha1.RedisCluster, p *plan.Plan, stepIndex int, ns, podName string) bool {
	return precededEnsureNode(p, stepIndex, ns, podName) || podInTopology(cluster, podName)
}

func setSlotNodeOnHealthyMasters(ctx context.Context, factory redis.Factory, entries []rediscluster.Entry, slot int, targetID string) error {
	for _, entry := range entries {
		if !entry.IsMaster() || !entry.Healthy() {
			continue
		}
		addr := rediscluster.RedisAddrFromClusterAddr(entry.Addr)
		rc, err := factory(addr)
		if err != nil {
			return fmt.Errorf("build redis client for %s: %w", addr, err)
		}
		if err := rc.ClusterSetSlotNode(ctx, slot, targetID); err != nil {
			rc.Close()
			return err
		}
		if err := rc.Close(); err != nil {
			return err
		}
	}
	return nil
}
