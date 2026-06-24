package plan

import "fmt"

type simulatedNode struct {
	exists        bool
	ready         bool
	clusterMember bool
	role          string
	replicaOf     string
	slots         map[int]struct{}
}

type planSimulator struct {
	spec       ClusterSpec
	nodes      map[string]*simulatedNode
	slotOwners map[int]string
	healMode   bool
}

func simulatePlan(p *Plan, ctx ValidationContext) error {
	s := newPlanSimulator(ctx)
	for i, step := range p.Steps {
		if err := s.apply(step, p, i); err != nil {
			return fmt.Errorf("step %q: %w", step.ID, err)
		}
		if i != len(p.Steps)-1 {
			if err := s.checkInvariants(); err != nil {
				return fmt.Errorf("step %q: %w", step.ID, err)
			}
		}
	}
	return nil
}

func newPlanSimulator(ctx ValidationContext) *planSimulator {
	s := &planSimulator{
		spec:       ctx.Spec,
		nodes:      map[string]*simulatedNode{},
		slotOwners: map[int]string{},
		healMode:   ctx.healMode,
	}
	if len(ctx.ObservedNodes) > 0 {
		s.addObservedNodes(ctx.ObservedNodes)
		return s
	}
	s.addTopology(ctx.Topology)
	return s
}

func (s *planSimulator) addTopology(topology *ClusterTopology) {
	if topology == nil {
		return
	}
	for _, sh := range topology.Shards {
		m := s.ensureExistingNode(sh.Master.Pod)
		m.ready = sh.Master.Ready
		m.clusterMember = true
		m.role = "master"
		if slots, err := parseSlots(sh.Master.Slots); err == nil {
			for slot := range slots {
				m.slots[slot] = struct{}{}
				s.slotOwners[slot] = sh.Master.Pod
			}
		}
		for _, r := range sh.Replicas {
			replica := s.ensureExistingNode(r.Pod)
			replica.ready = r.Ready
			replica.clusterMember = true
			replica.role = "replica"
			replica.replicaOf = sh.Master.Pod
		}
	}
}

func (s *planSimulator) addObservedNodes(nodes []ObservedNode) {
	nodeIDToPod := map[string]string{}
	for _, observed := range nodes {
		if observed.NodeID != "" && observed.Pod != "" {
			nodeIDToPod[observed.NodeID] = observed.Pod
		}
	}
	for _, observed := range nodes {
		pod := observed.Pod
		if pod == "" {
			pod = observed.NodeID
		}
		if pod == "" {
			continue
		}
		n := s.ensureSimulatedNode(pod)
		n.exists = observed.PodExists
		n.ready = observed.Ready && !observed.Deleting
		n.clusterMember = observed.RedisSeen
		n.role = observed.Role
		if n.role == "" {
			n.role = "unknown"
		}
		if observed.MasterPod != "" {
			n.replicaOf = observed.MasterPod
		} else if observed.MasterID != "" {
			n.replicaOf = nodeIDToPod[observed.MasterID]
		}
		if observed.Role == "master" && observed.Slots != "" {
			if slots, err := parseSlots(observed.Slots); err == nil {
				for slot := range slots {
					n.slots[slot] = struct{}{}
					s.slotOwners[slot] = pod
				}
			}
		}
	}
}

func (s *planSimulator) ensureSimulatedNode(pod string) *simulatedNode {
	n := s.nodes[pod]
	if n == nil {
		n = &simulatedNode{slots: map[int]struct{}{}}
		s.nodes[pod] = n
	}
	return n
}

func (s *planSimulator) ensureExistingNode(pod string) *simulatedNode {
	n := s.ensureSimulatedNode(pod)
	n.exists = true
	return n
}

func (s *planSimulator) apply(step Step, p *Plan, stepIndex int) error {
	switch step.Action {
	case ActionEnsureNode:
		return s.ensureNode(step)
	case ActionWaitNodeReady:
		return s.waitNodeReady(step)
	case ActionMeetNode:
		return s.meetNode(step)
	case ActionReplicateNode:
		return s.replicateNode(step)
	case ActionAddSlots:
		return s.addSlots(step)
	case ActionMigrateSlots:
		return s.migrateSlots(step)
	case ActionForgetNode:
		return s.forgetNode(step)
	case ActionDeleteNode:
		return s.deleteNode(step, p, stepIndex)
	case ActionVerifyCluster:
		return s.verifyCluster(step)
	default:
		return fmt.Errorf("action %q cannot be simulated", step.Action)
	}
}

func (s *planSimulator) ensureNode(step Step) error {
	pod, ok := paramString(step.Params, "pod")
	if !ok || pod == "" {
		return fmt.Errorf("EnsureNode requires a non-empty pod param")
	}
	if image, _ := paramString(step.Params, "image"); image != s.spec.Image {
		return verr(fmt.Sprintf("Set image to %q", s.spec.Image), "image %q must equal spec.image %q", image, s.spec.Image)
	}
	if memorySize, _ := paramString(step.Params, "memorySize"); memorySize != s.spec.MemorySize {
		return verr(fmt.Sprintf("Set memorySize to %q", s.spec.MemorySize), "memorySize %q must equal spec.memorySize %q", memorySize, s.spec.MemorySize)
	}
	s.ensureExistingNode(pod)
	return nil
}

func (s *planSimulator) waitNodeReady(step Step) error {
	pod, ok := paramString(step.Params, "pod")
	if !ok || pod == "" {
		return fmt.Errorf("WaitNodeReady requires a non-empty pod param")
	}
	n := s.nodes[pod]
	if n == nil || !n.exists {
		return verr(fmt.Sprintf("Add an EnsureNode step for pod %q before this WaitNodeReady", pod), "pod %q was not declared by EnsureNode", pod)
	}
	n.ready = true
	return nil
}

func (s *planSimulator) meetNode(step Step) error {
	sourcePod, ok := paramString(step.Params, "sourcePod")
	if !ok || sourcePod == "" {
		return fmt.Errorf("MeetNode requires a non-empty sourcePod param")
	}
	targetPod, ok := paramString(step.Params, "targetPod")
	if !ok || targetPod == "" {
		return fmt.Errorf("MeetNode requires a non-empty targetPod param")
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
	source.clusterMember = true
	target.clusterMember = true
	if source.role == "" {
		source.role = "master"
	}
	if target.role == "" {
		target.role = "master"
	}
	return nil
}

func (s *planSimulator) replicateNode(step Step) error {
	masterPod, ok := paramString(step.Params, "masterPod")
	if !ok || masterPod == "" {
		return fmt.Errorf("ReplicateNode requires a non-empty masterPod param")
	}
	replicaPod, ok := paramString(step.Params, "replicaPod")
	if !ok || replicaPod == "" {
		return fmt.Errorf("ReplicateNode requires a non-empty replicaPod param")
	}
	master := s.nodes[masterPod]
	if master == nil || !master.ready || !master.clusterMember {
		return verr(fmt.Sprintf("Add a WaitNodeReady step for pod %q before this ReplicateNode", masterPod), "master pod %q is not a ready cluster member", masterPod)
	}
	if master.role == "" {
		master.role = "master"
	}
	if master.role != "master" {
		return verr(fmt.Sprintf("Pod %q must be a master; check the ReplicateNode assignment", masterPod), "master pod %q is not a master", masterPod)
	}
	replica := s.nodes[replicaPod]
	if replica == nil || !replica.ready || !replica.clusterMember {
		return verr(fmt.Sprintf("Add a MeetNode step for pod %q before this ReplicateNode", replicaPod), "replica pod %q is not a ready cluster member", replicaPod)
	}
	if len(replica.slots) > 0 {
		return verr(fmt.Sprintf("Pod %q already owns slots; use MigrateSlots to transfer them away first", replicaPod), "replica pod %q already owns slots", replicaPod)
	}
	replica.role = "replica"
	replica.replicaOf = masterPod
	return nil
}

func (s *planSimulator) addSlots(step Step) error {
	pod, ok := paramString(step.Params, "pod")
	if !ok || pod == "" {
		return fmt.Errorf("AddSlots requires a non-empty pod param")
	}
	n := s.nodes[pod]
	if n == nil || !n.ready || !n.clusterMember {
		return verr(fmt.Sprintf("Add WaitNodeReady and MeetNode steps for pod %q before this AddSlots", pod), "target pod %q is not a ready cluster member", pod)
	}
	if n.role == "" {
		n.role = "master"
	}
	if n.role != "master" {
		return verr(fmt.Sprintf("Pod %q must be a master to own slots; check the ReplicateNode assignment", pod), "target pod %q is not a master", pod)
	}
	if s.replicaCount(pod) == 0 {
		return verr(fmt.Sprintf("Add a ReplicateNode step to give master %q a replica before assigning slots", pod), "target master %q has no replica", pod)
	}
	slotsStr, ok := paramString(step.Params, "slots")
	if !ok || slotsStr == "" {
		return fmt.Errorf("AddSlots requires a non-empty slots param")
	}
	slots, err := parseSlots(slotsStr)
	if err != nil {
		return err
	}
	for slot := range slots {
		if owner, exists := s.slotOwners[slot]; exists {
			return verr(fmt.Sprintf("Slot %d is already owned by %q; use MigrateSlots to transfer it instead of AddSlots", slot, owner), "slot %d already belongs to pod %q", slot, owner)
		}
	}
	for slot := range slots {
		n.slots[slot] = struct{}{}
		s.slotOwners[slot] = pod
	}
	return nil
}

func (s *planSimulator) migrateSlots(step Step) error {
	sourcePod, ok := paramString(step.Params, "sourcePod")
	if !ok || sourcePod == "" {
		return fmt.Errorf("MigrateSlots requires a non-empty sourcePod param")
	}
	targetPod, ok := paramString(step.Params, "targetPod")
	if !ok || targetPod == "" {
		return fmt.Errorf("MigrateSlots requires a non-empty targetPod param")
	}
	if sourcePod == targetPod {
		return fmt.Errorf("sourcePod and targetPod must differ")
	}
	source := s.nodes[sourcePod]
	if source == nil || !source.ready || !source.clusterMember || source.role != "master" {
		return verr(fmt.Sprintf("Pod %q must be a ready master to migrate slots from; add WaitNodeReady if needed", sourcePod), "source pod %q is not a ready master", sourcePod)
	}
	target := s.nodes[targetPod]
	if target == nil || !target.ready || !target.clusterMember || target.role != "master" {
		return verr(fmt.Sprintf("Pod %q must be a ready master to receive slots; add WaitNodeReady and MeetNode if needed", targetPod), "target pod %q is not a ready master", targetPod)
	}
	if s.replicaCount(targetPod) == 0 {
		return verr(fmt.Sprintf("Add a ReplicateNode step to give target master %q a replica before migrating slots", targetPod), "target master %q has no replica", targetPod)
	}
	slotsStr, ok := paramString(step.Params, "slots")
	if !ok || slotsStr == "" {
		return fmt.Errorf("MigrateSlots requires a non-empty slots param")
	}
	slots, err := parseSlots(slotsStr)
	if err != nil {
		return err
	}
	for slot := range slots {
		owner, exists := s.slotOwners[slot]
		if !exists {
			return fmt.Errorf("slot %d has no owner", slot)
		}
		if owner != sourcePod && owner != targetPod {
			return verr(fmt.Sprintf("Slot %d belongs to %q; migrate from its current owner %q to %q", slot, owner, owner, targetPod), "slot %d belongs to pod %q, not source %q or target %q", slot, owner, sourcePod, targetPod)
		}
	}
	for slot := range slots {
		if s.slotOwners[slot] == targetPod {
			continue
		}
		delete(source.slots, slot)
		target.slots[slot] = struct{}{}
		s.slotOwners[slot] = targetPod
	}
	return nil
}

func (s *planSimulator) forgetNode(step Step) error {
	pod, ok := paramString(step.Params, "pod")
	if !ok || pod == "" {
		return fmt.Errorf("ForgetNode requires a non-empty pod param")
	}
	key := pod
	n := s.nodes[key]
	if n == nil {
		// The pod may be gone (ghost); fall back to lastKnownNodeId, which is
		// the key under which addObservedNodes registered the node when its
		// pod name was empty.
		if id, ok := paramString(step.Params, "lastKnownNodeId"); ok && id != "" {
			key = id
			n = s.nodes[key]
		}
	}
	if n == nil || !n.clusterMember {
		return fmt.Errorf("pod %q is not a known cluster member", pod)
	}
	if len(n.slots) > 0 {
		return fmt.Errorf("pod %q still owns slots", pod)
	}
	n.clusterMember = false
	return nil
}

func (s *planSimulator) deleteNode(step Step, p *Plan, stepIndex int) error {
	pod, ok := paramString(step.Params, "pod")
	if !ok || pod == "" {
		return fmt.Errorf("DeleteNode requires a non-empty pod param")
	}
	n := s.nodes[pod]
	if n == nil {
		// Pod not in simulator: allow only when a preceding ForgetNode
		// targeted this pod (cleaning up a ghost whose pod name was lost).
		if !precededAction(p, stepIndex, ActionForgetNode, "pod", pod) {
			return fmt.Errorf("pod %q is unknown", pod)
		}
		return nil
	}
	if n.clusterMember {
		return fmt.Errorf("pod %q is still an active cluster member", pod)
	}
	n.exists = false
	n.ready = false
	return nil
}

func (s *planSimulator) verifyCluster(step Step) error {
	expectedShards, ok := paramInt(step.Params, "expectedShards")
	if !ok {
		return fmt.Errorf("VerifyCluster requires integer expectedShards")
	}
	expectedReplicas, ok := paramInt(step.Params, "expectedReplicasPerShard")
	if !ok {
		return fmt.Errorf("VerifyCluster requires integer expectedReplicasPerShard")
	}
	if expectedShards != int(s.spec.Shards) {
		return verr(fmt.Sprintf("Set expectedShards to spec.shards value %d", s.spec.Shards), "expectedShards %d must equal spec.shards %d", expectedShards, s.spec.Shards)
	}
	if expectedReplicas != int(s.spec.ReplicasPerShard) {
		return verr(fmt.Sprintf("Set expectedReplicasPerShard to spec.replicasPerShard value %d", s.spec.ReplicasPerShard), "expectedReplicasPerShard %d must equal spec.replicasPerShard %d", expectedReplicas, s.spec.ReplicasPerShard)
	}
	masters := 0
	for pod, n := range s.nodes {
		if !n.exists || !n.clusterMember || n.role != "master" || len(n.slots) == 0 {
			continue
		}
		masters++
		if s.replicaCount(pod) != expectedReplicas {
			return fmt.Errorf("master %q has %d replicas, expected %d", pod, s.replicaCount(pod), expectedReplicas)
		}
	}
	if masters != expectedShards {
		return verr(fmt.Sprintf("Found %d slot-owning masters; add EnsureNode steps for missing masters", masters), "found %d slot-owning masters, expected %d", masters, expectedShards)
	}
	if len(s.slotOwners) != 16384 {
		return verr(fmt.Sprintf("Slot coverage is %d/16384; add AddSlots or MigrateSlots steps to cover all slots 0-16383", len(s.slotOwners)), "slot coverage is %d, expected 16384", len(s.slotOwners))
	}
	return s.checkInvariants()
}

func (s *planSimulator) checkInvariants() error {
	for pod, n := range s.nodes {
		if !n.exists || !n.clusterMember {
			continue
		}
		if n.role == "replica" && len(n.slots) > 0 {
			return verr(fmt.Sprintf("Replica %q owns slots; use MigrateSlots to transfer them to a master first", pod), "replica pod %q owns slots", pod)
		}
		if len(n.slots) > 0 {
			if n.role != "master" {
				return verr(fmt.Sprintf("Pod %q owns slots but is not a master; set its role via ReplicateNode", pod), "slot-owning pod %q is not a master", pod)
			}
			if !s.healMode && s.replicaCount(pod) == 0 {
				return verr(fmt.Sprintf("Master %q owns slots but has no replica; add a ReplicateNode step for a replica", pod), "slot-owning master %q has no replica", pod)
			}
		}
	}
	seen := map[int]string{}
	for pod, n := range s.nodes {
		if !n.exists || !n.clusterMember {
			continue
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
	count := 0
	for _, n := range s.nodes {
		if n.exists && n.clusterMember && n.role == "replica" && n.replicaOf == masterPod {
			count++
		}
	}
	return count
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
		i := int(v)
		return i, float64(i) == v
	default:
		return 0, false
	}
}
