package observor

import (
	"strconv"
	"strings"
)

const (
	NodeRoleMaster  = "master"
	NodeRoleReplica = "replica"
)

// ClusterObservation描述目前Redis Cluster的状态，解析并整合后
type ClusterObservation struct {
	Shards             []Shard
	UncategorizedNodes map[string]Node // 没有被归类到任何分片的节点
}

// Shard描述Redis Cluster中一个分片的状态
type Shard struct {
	Slots        [][2]int        // 分片的槽位范围,数字是一个闭区间, e.g. [[0, 5460], [5461, 10922], [10923, 16383]]
	MasterNodeID string          // 分片的主节点ID
	Nodes        map[string]Node // 属于此分片中的节点
}

// Node描述Redis Cluster中一个节点的状态
type Node struct {
	NodeID  string
	PodName string
	Role    string
	Flags   []string
}

// ParseClusterNodesInfo解析Redis Cluster的CLUSTER NODES命令输出，返回ClusterObservation结构体
// 1. 首先解析出有slot的normal的Master节点
// 2. 然后找到上一步的每个Master节点的各自的Replica节点
func ParseClusterNodesInfo(info string) ClusterObservation {
	lines := strings.Split(info, "\n")
	type rawNode struct {
		NodeID   string
		PodName  string
		Role     string
		Flags    []string
		MasterID string
		Slots    [][2]int
	}
	var rawNodes []rawNode

	for _, line := range lines {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Split(line, " ")
		if len(fields) < 8 {
			continue
		}

		nodeID := fields[0]
		addrField := fields[1]
		podName := ""
		if commaIdx := strings.Index(addrField, ","); commaIdx != -1 {
			podName = addrField[commaIdx+1:]
		}

		flags := strings.Split(fields[2], ",")
		role := NodeRoleReplica
		isMaster := false
		for _, f := range flags {
			if f == "master" {
				role = NodeRoleMaster
				isMaster = true
			}
		}
		if !isMaster {
			for _, f := range flags {
				if f == "slave" || f == "replica" {
					role = NodeRoleReplica
				}
			}
		}

		masterID := fields[3]
		var slots [][2]int
		for i := 8; i < len(fields); i++ {
			slotField := fields[i]
			if strings.HasPrefix(slotField, "[") {
				continue
			}
			parts := strings.Split(slotField, "-")
			if len(parts) == 1 {
				if s, err := strconv.Atoi(parts[0]); err == nil {
					slots = append(slots, [2]int{s, s})
				}
			} else if len(parts) == 2 {
				s1, err1 := strconv.Atoi(parts[0])
				s2, err2 := strconv.Atoi(parts[1])
				if err1 == nil && err2 == nil {
					slots = append(slots, [2]int{s1, s2})
				}
			}
		}

		rawNodes = append(rawNodes, rawNode{
			NodeID:   nodeID,
			PodName:  podName,
			Role:     role,
			Flags:    flags,
			MasterID: masterID,
			Slots:    slots,
		})
	}

	var shards []Shard
	categorized := make(map[string]struct{})

	// 1. 首先解析出有slot的normal的Master节点
	for _, rn := range rawNodes {
		isMaster := false
		isFail := false
		for _, f := range rn.Flags {
			if f == "master" {
				isMaster = true
			}
			if f == "fail" || f == "fail?" || f == "handshake" || f == "noaddr" {
				isFail = true
			}
		}

		if isMaster && !isFail && len(rn.Slots) > 0 {
			shard := Shard{
				Slots:        rn.Slots,
				MasterNodeID: rn.NodeID,
				Nodes:        make(map[string]Node),
			}
			shard.Nodes[rn.NodeID] = Node{
				NodeID:  rn.NodeID,
				PodName: rn.PodName,
				Role:    rn.Role,
				Flags:   rn.Flags,
			}
			categorized[rn.NodeID] = struct{}{}
			shards = append(shards, shard)
		}
	}

	// 2. 然后找到上一步的每个Master节点的各自的Replica节点
	for i := range shards {
		for _, rn := range rawNodes {
			if rn.MasterID == shards[i].MasterNodeID {
				isFail := false
				for _, f := range rn.Flags {
					if f == "handshake" || f == "noaddr" {
						isFail = true
					}
				}
				if !isFail {
					shards[i].Nodes[rn.NodeID] = Node{
						NodeID:  rn.NodeID,
						PodName: rn.PodName,
						Role:    rn.Role,
						Flags:   rn.Flags,
					}
					categorized[rn.NodeID] = struct{}{}
				}
			}
		}
	}

	uncategorized := make(map[string]Node)
	for _, rn := range rawNodes {
		if _, ok := categorized[rn.NodeID]; !ok {
			uncategorized[rn.NodeID] = Node{
				NodeID:  rn.NodeID,
				PodName: rn.PodName,
				Role:    rn.Role,
				Flags:   rn.Flags,
			}
		}
	}

	return ClusterObservation{
		Shards:             shards,
		UncategorizedNodes: uncategorized,
	}
}
