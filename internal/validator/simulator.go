package validator

import (
	"fmt"

	"github.com/Zumium/llm-k8s-redis/internal/observor"
	"github.com/Zumium/llm-k8s-redis/internal/plan"
)

type simulatedNode struct {
	exists, ready, clusterMember, forgotten bool
	role, replicaOf                         string
	image                                   string
	slots                                   map[int]struct{}
}

type planSimulator struct {
	spec       plan.ClusterSpec
	nodes      map[string]*simulatedNode
	nodeIDs    map[string]string
	slotOwners map[int]string
}

func simulatePlan(spec plan.ClusterSpec, nodes []plan.ObservedNode, p *plan.Plan) error {
	s := newPlanSimulator(spec, nodes)
	for i, step := range p.Steps {
		if err := s.apply(step, p, i); err != nil {
			return fmt.Errorf("step %q: %w", step.ID, err)
		}
		if err := s.checkInvariants(); err != nil {
			return fmt.Errorf("step %q: %w", step.ID, err)
		}
	}
	return nil
}

func newPlanSimulator(spec plan.ClusterSpec, nodes []plan.ObservedNode) *planSimulator {
	s := &planSimulator{spec: spec, nodes: map[string]*simulatedNode{}, nodeIDs: map[string]string{}, slotOwners: map[int]string{}}
	s.addObservedNodes(nodes)
	return s
}

func observedKey(n plan.ObservedNode) string {
	if n.Pod != "" {
		return n.Pod
	}
	return n.NodeID
}

func (s *planSimulator) addObservedNodes(nodes []plan.ObservedNode) {
	for _, n := range nodes {
		key := observedKey(n)
		if key == "" {
			continue
		}
		if n.NodeID != "" {
			s.nodeIDs[n.NodeID] = key
		}
		s.ensureNodeKey(key)
	}
	for _, n := range nodes {
		key := observedKey(n)
		if key == "" {
			continue
		}
		sn := s.ensureNodeKey(key)
		if n.PodExists {
			sn.exists = true
			sn.image = n.Image
		}
		if n.RedisSeen {
			sn.clusterMember, sn.role, sn.ready = true, n.Role, observor.ObservedNodeHealthy(n)
			sn.replicaOf = n.MasterPod
			if sn.replicaOf == "" {
				sn.replicaOf = s.nodeIDs[n.MasterID]
			}
			if sn.replicaOf == "" {
				sn.replicaOf = n.MasterID
			}
		}
		if slots, err := parseSlots(n.Slots); err == nil {
			for slot := range slots {
				sn.slots[slot] = struct{}{}
				s.slotOwners[slot] = key
			}
		}
	}
}

func (s *planSimulator) ensureNodeKey(key string) *simulatedNode {
	if n := s.nodes[key]; n != nil {
		return n
	}
	n := &simulatedNode{slots: map[int]struct{}{}}
	s.nodes[key] = n
	return n
}

func (s *planSimulator) apply(step plan.Step, p *plan.Plan, idx int) error {
	switch step.Action {
	case plan.ActionEnsureNode:
		return s.ensureNode(step)
	case plan.ActionWaitNodeReady:
		return s.waitNodeReady(step)
	case plan.ActionMeetNode:
		return s.meetNode(step)
	case plan.ActionReplicateNode:
		return s.replicateNode(step)
	case plan.ActionAddSlots:
		return s.addSlots(step)
	case plan.ActionMigrateSlots:
		return s.migrateSlots(step)
	case plan.ActionForgetNode:
		return s.forgetNode(step)
	case plan.ActionDeleteNode:
		return s.deleteNode(step, p, idx)
	case plan.ActionVerifyCluster:
		return s.verifyCluster(step)
	default:
		return fmt.Errorf("action %q cannot be simulated", step.Action)
	}
}

func reqP(params map[string]any, key, action string) (string, error) {
	if v, ok := paramString(params, key); ok && v != "" {
		return v, nil
	}
	return "", fmt.Errorf("%s requires a non-empty %s param", action, key)
}

func reqSlots(params map[string]any, action string) (map[int]struct{}, error) {
	s, err := reqP(params, "slots", action)
	if err != nil {
		return nil, err
	}
	return parseSlots(s)
}

func (s *planSimulator) readyMaster(pod, hint string) (*simulatedNode, error) {
	n := s.nodes[pod]
	if n == nil || !n.ready || !n.clusterMember {
		return nil, verr(hint, "pod %q is not a ready cluster member", pod)
	}
	if n.role != "master" {
		return nil, verr(fmt.Sprintf("Pod %q must be a master", pod), "pod %q is not a master", pod)
	}
	return n, nil
}

func (s *planSimulator) ensureNode(step plan.Step) error {
	pod, err := reqP(step.Params, "pod", "EnsureNode")
	if err != nil {
		return err
	}
	image, err := reqP(step.Params, "image", "EnsureNode")
	if err != nil {
		return err
	}
	if image != s.spec.Image {
		return fmt.Errorf("EnsureNode image %q must equal spec image %q", image, s.spec.Image)
	}
	n := s.ensureNodeKey(pod)
	if !n.exists {
		n.image = image
	}
	n.exists = true
	return nil
}

func (s *planSimulator) waitNodeReady(step plan.Step) error {
	pod, err := reqP(step.Params, "pod", "WaitNodeReady")
	if err != nil {
		return err
	}
	n := s.nodes[pod]
	if n == nil || !n.exists {
		return verr(fmt.Sprintf("Add an EnsureNode step for pod %q before this WaitNodeReady", pod), "pod %q was not declared by EnsureNode", pod)
	}
	n.ready = true
	return nil
}

func (s *planSimulator) meetNode(step plan.Step) error {
	sourcePod, err := reqP(step.Params, "sourcePod", "MeetNode")
	if err != nil {
		return err
	}
	targetPod, err := reqP(step.Params, "targetPod", "MeetNode")
	if err != nil {
		return err
	}
	if sourcePod == targetPod {
		return fmt.Errorf("sourcePod and targetPod must differ")
	}
	source := s.nodes[sourcePod]
	if source == nil || !source.ready {
		return verr(fmt.Sprintf("Add a WaitNodeReady step for pod %q before this MeetNode", sourcePod), "source pod %q is not ready", sourcePod)
	}
	target := s.nodes[targetPod]
	if target == nil || !target.ready {
		return verr(fmt.Sprintf("Add a WaitNodeReady step for pod %q before this MeetNode", targetPod), "target pod %q is not ready", targetPod)
	}
	source.clusterMember, target.clusterMember = true, true
	if source.role == "" {
		source.role = "master"
	}
	if target.role == "" {
		target.role = "master"
	}
	return nil
}

func (s *planSimulator) replicateNode(step plan.Step) error {
	masterPod, err := reqP(step.Params, "masterPod", "ReplicateNode")
	if err != nil {
		return err
	}
	replicaPod, err := reqP(step.Params, "replicaPod", "ReplicateNode")
	if err != nil {
		return err
	}
	master := s.nodes[masterPod]
	if master == nil || !master.ready || !master.clusterMember {
		return verr(fmt.Sprintf("Add a WaitNodeReady step for pod %q before this ReplicateNode", masterPod), "master pod %q is not a ready cluster member", masterPod)
	}
	if master.role != "" && master.role != "master" {
		return verr(fmt.Sprintf("Pod %q must be a master; check the ReplicateNode assignment", masterPod), "master pod %q is not a master", masterPod)
	}
	master.role = "master"
	replica := s.nodes[replicaPod]
	if replica == nil || !replica.ready || !replica.clusterMember {
		return verr(fmt.Sprintf("Add a MeetNode step for pod %q before this ReplicateNode", replicaPod), "replica pod %q is not a ready cluster member", replicaPod)
	}
	if len(replica.slots) > 0 {
		return verr(fmt.Sprintf("Pod %q already owns slots; use MigrateSlots to transfer them away first", replicaPod), "replica pod %q already owns slots", replicaPod)
	}
	replica.role, replica.replicaOf = "replica", masterPod
	return nil
}

func (s *planSimulator) addSlots(step plan.Step) error {
	pod, err := reqP(step.Params, "pod", "AddSlots")
	if err != nil {
		return err
	}
	n, err := s.readyMaster(pod, fmt.Sprintf("Add WaitNodeReady and MeetNode steps for pod %q before this AddSlots", pod))
	if err != nil {
		return err
	}
	if s.replicaCount(pod) == 0 {
		return verr(fmt.Sprintf("Add a ReplicateNode step to give master %q a replica before assigning slots", pod), "target master %q has no replica", pod)
	}
	slots, err := reqSlots(step.Params, "AddSlots")
	if err != nil {
		return err
	}
	for slot := range slots {
		if owner, ok := s.slotOwners[slot]; ok {
			return verr(fmt.Sprintf("Slot %d is already owned by %q; use MigrateSlots to transfer it instead of AddSlots", slot, owner), "slot %d already belongs to pod %q", slot, owner)
		}
		n.slots[slot] = struct{}{}
		s.slotOwners[slot] = pod
	}
	return nil
}

func (s *planSimulator) migrateSlots(step plan.Step) error {
	sourcePod, err := reqP(step.Params, "sourcePod", "MigrateSlots")
	if err != nil {
		return err
	}
	targetPod, err := reqP(step.Params, "targetPod", "MigrateSlots")
	if err != nil {
		return err
	}
	if sourcePod == targetPod {
		return fmt.Errorf("sourcePod and targetPod must differ")
	}
	source, err := s.readyMaster(sourcePod, fmt.Sprintf("Pod %q must be a ready master to migrate slots from; add WaitNodeReady if needed", sourcePod))
	if err != nil {
		return err
	}
	target, err := s.readyMaster(targetPod, fmt.Sprintf("Pod %q must be a ready master to receive slots; add WaitNodeReady and MeetNode if needed", targetPod))
	if err != nil {
		return err
	}
	if s.replicaCount(targetPod) == 0 {
		return verr(fmt.Sprintf("Add a ReplicateNode step to give target master %q a replica before migrating slots", targetPod), "target master %q has no replica", targetPod)
	}
	slots, err := reqSlots(step.Params, "MigrateSlots")
	if err != nil {
		return err
	}
	for slot := range slots {
		owner, ok := s.slotOwners[slot]
		if !ok {
			return fmt.Errorf("slot %d has no owner", slot)
		}
		if owner != sourcePod && owner != targetPod {
			return verr(fmt.Sprintf("Slot %d belongs to %q; migrate from its current owner %q to %q", slot, owner, owner, targetPod), "slot %d belongs to pod %q, not source %q or target %q", slot, owner, sourcePod, targetPod)
		}
		if owner != sourcePod {
			continue
		}
		delete(source.slots, slot)
		target.slots[slot] = struct{}{}
		s.slotOwners[slot] = targetPod
	}
	return nil
}

func (s *planSimulator) forgetNode(step plan.Step) error {
	pod, _ := paramString(step.Params, "pod")
	id, _ := paramString(step.Params, "lastKnownNodeId")
	key, n := pod, (*simulatedNode)(nil)
	for _, c := range []string{s.nodeIDs[id], id, pod} {
		if c != "" {
			if cn := s.nodes[c]; cn != nil {
				key, n = c, cn
				break
			}
		}
	}
	if n == nil || !n.clusterMember {
		return fmt.Errorf("ForgetNode target %q is not a known cluster member", key)
	}
	if len(n.slots) > 0 {
		return fmt.Errorf("ForgetNode target %q still owns slots", key)
	}
	n.clusterMember, n.forgotten = false, true
	return nil
}

func (s *planSimulator) deleteNode(step plan.Step, p *plan.Plan, idx int) error {
	pod, err := reqP(step.Params, "pod", "DeleteNode")
	if err != nil {
		return err
	}
	n := s.nodes[pod]
	if n == nil {
		if !precededAction(p, idx, plan.ActionForgetNode, "pod", pod) {
			return fmt.Errorf("pod %q is unknown", pod)
		}
		return nil
	}
	if n.clusterMember {
		return fmt.Errorf("pod %q is still an active cluster member", pod)
	}
	n.exists, n.ready = false, false
	return nil
}

func (s *planSimulator) verifyCluster(step plan.Step) error {
	expectedShards, ok := paramInt(step.Params, "expectedShards")
	if !ok {
		return fmt.Errorf("VerifyCluster requires integer expectedShards")
	}
	expectedReplicas, ok := paramInt(step.Params, "expectedReplicasPerShard")
	if !ok {
		return fmt.Errorf("VerifyCluster requires integer expectedReplicasPerShard")
	}
	masters := 0
	for pod, n := range s.nodes {
		if n.exists && !n.clusterMember && !n.forgotten {
			return fmt.Errorf("managed pod %q is not a Redis cluster member", pod)
		}
		if !n.exists || !n.clusterMember || !n.ready {
			continue
		}
		if n.role == "master" && len(n.slots) == 0 {
			return fmt.Errorf("managed master %q owns no slots; reuse it or delete it before VerifyCluster", pod)
		}
		if n.role != "master" {
			continue
		}
		masters++
		if rc := s.replicaCount(pod); rc != expectedReplicas {
			return fmt.Errorf("master %q has %d replicas, expected %d", pod, rc, expectedReplicas)
		}
	}
	if masters != expectedShards {
		return verr(fmt.Sprintf("Found %d slot-owning masters; add EnsureNode steps for missing masters", masters), "found %d slot-owning masters, expected %d", masters, expectedShards)
	}
	if len(s.slotOwners) != 16384 {
		return verr(fmt.Sprintf("Slot coverage is %d/16384; add AddSlots or MigrateSlots steps to cover all slots 0-16383", len(s.slotOwners)), "slot coverage is %d, expected 16384", len(s.slotOwners))
	}
	for pod, n := range s.nodes {
		if n.exists && n.image == "" {
			return fmt.Errorf("managed pod %q has empty image", pod)
		}
		if n.exists && n.image != s.spec.Image {
			return fmt.Errorf("managed pod %q image %q does not match spec image %q", pod, n.image, s.spec.Image)
		}
		if n.clusterMember && !n.forgotten && (!n.exists || !n.ready) {
			return fmt.Errorf("redis member %q must be forgotten before VerifyCluster", pod)
		}
	}
	return s.checkInvariants()
}

func (s *planSimulator) checkInvariants() error {
	seen := map[int]string{}
	for pod, n := range s.nodes {
		if !n.exists || !n.clusterMember {
			continue
		}
		if len(n.slots) > 0 && n.role != "master" {
			return verr(fmt.Sprintf("Pod %q owns slots but is not a master", pod), "slot-owning pod %q is not a master (role=%q)", pod, n.role)
		}
		for slot := range n.slots {
			if prev, ok := seen[slot]; ok {
				return fmt.Errorf("slot %d belongs to both %q and %q", slot, prev, pod)
			}
			seen[slot] = pod
			if owner, ok := s.slotOwners[slot]; !ok || owner != pod {
				return fmt.Errorf("slot %d owner index is inconsistent", slot)
			}
		}
	}
	return nil
}

func (s *planSimulator) replicaCount(masterPod string) int {
	c := 0
	for _, n := range s.nodes {
		if n.exists && n.clusterMember && n.ready && n.role == "replica" && n.replicaOf == masterPod {
			c++
		}
	}
	return c
}

func paramInt(params map[string]any, key string) (int, bool) {
	if params == nil {
		return 0, false
	}
	switch v := params[key].(type) {
	case int:
		return v, true
	case int32:
		return int(v), true
	case int64:
		return int(v), true
	case float64:
		if i := int(v); float64(i) == v {
			return i, true
		}
	}
	return 0, false
}
