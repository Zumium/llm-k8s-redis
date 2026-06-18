package controller

import (
	"testing"
)

func TestParseSlotSpec_SingleRange(t *testing.T) {
	got, err := parseSlotSpec("0-8191")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(got) != 8192 || got[0] != 0 || got[len(got)-1] != 8191 {
		end := 3
		if len(got) < end {
			end = len(got)
		}
		t.Errorf("expected 8192 slots 0..8191, got %d %v", len(got), got[:end])
	}
}

func TestParseSlotSpec_MixedAndDedup(t *testing.T) {
	got, err := parseSlotSpec("0-2,1,5")
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	want := []int{0, 1, 2, 5}
	if len(got) != len(want) {
		t.Fatalf("expected %d slots, got %d (%v)", len(want), len(got), got)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("idx %d: want %d got %d", i, want[i], got[i])
		}
	}
}

func TestParseSlotSpec_OutOfBoundsFails(t *testing.T) {
	if _, err := parseSlotSpec("0-16384"); err == nil {
		t.Error("expected error for slot 16384")
	}
	if _, err := parseSlotSpec("-1"); err == nil {
		t.Error("expected error for slot -1")
	}
}

func TestParseSlotSpec_MigratingTokenFails(t *testing.T) {
	if _, err := parseSlotSpec("[5->-aaa]"); err == nil {
		t.Error("expected error for migrating token in spec")
	}
}

func TestSlotOwnership(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "aaa", Slots: []string{"0-100", "5000"}},
		{ID: "bbb", Slots: []string{"101-200"}},
	}
	owner, err := slotOwnership(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if owner[0] != "aaa" || owner[100] != "aaa" || owner[5000] != "aaa" {
		t.Errorf("expected aaa to own 0,100,5000, got %v", owner)
	}
	if owner[150] != "bbb" {
		t.Errorf("expected bbb to own 150, got %v", owner[150])
	}
	if _, ok := owner[9999]; ok {
		t.Error("expected slot 9999 unassigned")
	}
}

func TestSlotOwnership_DuplicateOwnerFails(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "aaa", Slots: []string{"0-100"}},
		{ID: "bbb", Slots: []string{"50"}},
	}
	if _, err := slotOwnership(entries); err == nil {
		t.Error("expected error when slot owned by two nodes")
	}
}

func TestSlotOwnership_SkipsMigratingTokens(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "aaa", Slots: []string{"0-100", "[200->-bbb]"}},
	}
	owner, err := slotOwnership(entries)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if _, ok := owner[200]; ok {
		t.Error("expected migrating slot 200 not in ownership map")
	}
	if owner[50] != "aaa" {
		t.Errorf("expected aaa to own 50, got %v", owner[50])
	}
}

func TestMigratingSlots(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "aaa", Slots: []string{"0-100", "[200->-bbb]", "[300-<-ccc]"}},
		{ID: "bbb", Slots: []string{"[500-510->-ddd]"}},
	}
	got := migratingSlots(entries)
	for _, s := range []int{200, 300, 500, 510} {
		if _, ok := got[s]; !ok {
			t.Errorf("expected slot %d in migrating set, got %v", s, got)
		}
	}
	for _, s := range []int{0, 100, 511, 9999} {
		if _, ok := got[s]; ok {
			t.Errorf("expected slot %d not in migrating set", s)
		}
	}
}

func TestMasterHasHealthyReplica(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "aaa", MasterID: "-", LinkState: "connected"},
		{ID: "bbb", MasterID: "aaa", LinkState: "connected"},
	}
	if !masterHasHealthyReplica(entries, "aaa") {
		t.Error("expected aaa to have a healthy replica")
	}
}

func TestMasterHasHealthyReplica_FailedReplica(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "aaa", MasterID: "-", LinkState: "connected"},
		{ID: "bbb", MasterID: "aaa", Flags: []string{"fail"}, LinkState: "connected"},
	}
	if masterHasHealthyReplica(entries, "aaa") {
		t.Error("expected aaa to have NO healthy replica when its only replica is failed")
	}
}

func TestMasterHasHealthyReplica_Disconnected(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "aaa", MasterID: "-", LinkState: "connected"},
		{ID: "bbb", MasterID: "aaa", LinkState: "disconnected"},
	}
	if masterHasHealthyReplica(entries, "aaa") {
		t.Error("expected aaa to have NO healthy replica when its replica is disconnected")
	}
}

func TestClusterNodeEntry_Healthy(t *testing.T) {
	if !(clusterNodeEntry{MasterID: "-", LinkState: "connected"}).healthy() {
		t.Error("expected connected master healthy")
	}
	if (clusterNodeEntry{MasterID: "-", Flags: []string{"fail"}, LinkState: "connected"}).healthy() {
		t.Error("expected failed node not healthy")
	}
	if (clusterNodeEntry{MasterID: "-", LinkState: "disconnected"}).healthy() {
		t.Error("expected disconnected node not healthy")
	}
}
