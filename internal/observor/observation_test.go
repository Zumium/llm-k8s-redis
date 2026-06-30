package observor

import (
	"testing"
)

func TestParseClusterNodesInfo(t *testing.T) {
	tests := []struct {
		name                 string
		input                string
		expectedShards       int
		expectedUncategorized int
		validate             func(t *testing.T, obs ClusterObservation)
	}{
		{
			name: "Normal scenario",
			input: `67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002,redis-cluster-0 master - 0 1426238316232 2 connected 5461-10922
292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 127.0.0.1:30003@31003,redis-cluster-1 master - 0 1426238318243 3 connected 10923-16383
6ec23923021cf3ffce45af1fac2d0163ee80f156 127.0.0.1:30005@31005,redis-cluster-2 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
824fea4215f448c279cdd525b84d6fb3a504d718 127.0.0.1:30006@31006,redis-cluster-3 slave 292f8b365bb7edb5e285caf0b7e6ddc7265d2f4f 0 1426238319253 6 connected
9e8e20084c7f0db4bb9dc10b42f65a468e82a65d 127.0.0.1:30001@31001,redis-cluster-4 myself,master - 0 0 1 connected 0-5460
1f49ceef0413009772ee5f32bdf6bd0b88939c06 127.0.0.1:30004@31004,redis-cluster-5 slave 9e8e20084c7f0db4bb9dc10b42f65a468e82a65d 0 1426238317237 4 connected`,
			expectedShards:       3,
			expectedUncategorized: 0,
			validate: func(t *testing.T, obs ClusterObservation) {
				var shard0 *Shard
				for i, s := range obs.Shards {
					if s.MasterNodeID == "9e8e20084c7f0db4bb9dc10b42f65a468e82a65d" {
						shard0 = &obs.Shards[i]
					}
				}
				if shard0 == nil {
					t.Fatalf("expected shard with master 9e8e20084c7f0db4bb9dc10b42f65a468e82a65d not found")
				}
				if len(shard0.Nodes) != 2 {
					t.Fatalf("expected shard 0 to have 2 nodes, got %d", len(shard0.Nodes))
				}
				if _, ok := shard0.Nodes["1f49ceef0413009772ee5f32bdf6bd0b88939c06"]; !ok {
					t.Fatalf("expected replica 1f49ceef0413009772ee5f32bdf6bd0b88939c06 to be in shard 0")
				}
				if len(shard0.Slots) != 1 || shard0.Slots[0] != [2]int{0, 5460} {
					t.Fatalf("expected slots [[0, 5460]], got %v", shard0.Slots)
				}
			},
		},
		{
			name: "Contains fail node (master failed and failover)",
			// 9e8e is a failed master with slots, 1f49 is the promoted master
			input: `67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002,redis-cluster-0 master - 0 1426238316232 2 connected 5461-10922
6ec23923021cf3ffce45af1fac2d0163ee80f156 127.0.0.1:30005@31005,redis-cluster-2 slave 67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 0 1426238316232 5 connected
9e8e20084c7f0db4bb9dc10b42f65a468e82a65d 127.0.0.1:30001@31001,redis-cluster-4 master,fail - 1426238316232 1426238316232 1 disconnected 0-5460
1f49ceef0413009772ee5f32bdf6bd0b88939c06 127.0.0.1:30004@31004,redis-cluster-5 myself,master - 0 0 4 connected 0-5460`,
			expectedShards:       2,
			expectedUncategorized: 1,
			validate: func(t *testing.T, obs ClusterObservation) {
				if _, ok := obs.UncategorizedNodes["9e8e20084c7f0db4bb9dc10b42f65a468e82a65d"]; !ok {
					t.Fatalf("expected failed master 9e8e20084c7f0db4bb9dc10b42f65a468e82a65d to be in UncategorizedNodes")
				}
				var newMasterShard *Shard
				for i, s := range obs.Shards {
					if s.MasterNodeID == "1f49ceef0413009772ee5f32bdf6bd0b88939c06" {
						newMasterShard = &obs.Shards[i]
					}
				}
				if newMasterShard == nil {
					t.Fatalf("expected promoted master 1f49ceef0413009772ee5f32bdf6bd0b88939c06 to be a shard")
				}
			},
		},
		{
			name: "Contains newly created empty slot master",
			input: `67ed2db8d677e59ec4a4cefb06858cf2a1a89fa1 127.0.0.1:30002@31002,redis-cluster-0 myself,master - 0 0 2 connected 0-16383
abcde2db8d677e59ec4a4cefb06858cf2a1a89fa 127.0.0.1:30003@31003,redis-cluster-1 master - 0 1426238316232 3 connected`,
			expectedShards:       1,
			expectedUncategorized: 1,
			validate: func(t *testing.T, obs ClusterObservation) {
				if _, ok := obs.UncategorizedNodes["abcde2db8d677e59ec4a4cefb06858cf2a1a89fa"]; !ok {
					t.Fatalf("expected empty slot master abcde2db8d677e59ec4a4cefb06858cf2a1a89fa to be in UncategorizedNodes")
				}
			},
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			obs := ParseClusterNodesInfo(tt.input)
			if len(obs.Shards) != tt.expectedShards {
				t.Fatalf("expected %d shards, got %d", tt.expectedShards, len(obs.Shards))
			}
			if len(obs.UncategorizedNodes) != tt.expectedUncategorized {
				t.Fatalf("expected %d uncategorized nodes, got %d", tt.expectedUncategorized, len(obs.UncategorizedNodes))
			}
			if tt.validate != nil {
				tt.validate(t, obs)
			}
		})
	}
}
