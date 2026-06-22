package rediscluster

import "testing"

func TestParseNodes_MasterAndReplica(t *testing.T) {
	raw := "aaa 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n" +
		"bbb 10.0.0.2:6379@16379 slave aaa 0 0 2 connected\n" +
		"\n"
	entries := ParseNodes(raw)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	m := entries[0]
	if m.ID != "aaa" || !m.IsMaster() || m.IsReplica() || !m.HasSlots() {
		t.Errorf("unexpected master entry %+v", m)
	}
	if len(m.Slots) != 1 || m.Slots[0] != "0-8191" {
		t.Errorf("expected slots [0-8191], got %v", m.Slots)
	}
	r := entries[1]
	if r.ID != "bbb" || r.IsMaster() || !r.IsReplica() || r.MasterID != "aaa" || r.HasSlots() {
		t.Errorf("unexpected replica entry %+v", r)
	}
}

func TestParseNodes_SkipsMalformedLines(t *testing.T) {
	raw := "short line\n" +
		"aaa 10.0.0.1:6379@16379 master - 0 0 1 connected\n" +
		"handshake\n"
	entries := ParseNodes(raw)
	if len(entries) != 1 {
		t.Fatalf("expected 1 valid entry, got %d", len(entries))
	}
	if entries[0].ID != "aaa" {
		t.Errorf("expected aaa, got %s", entries[0].ID)
	}
}

func TestFindByIP(t *testing.T) {
	entries := []Entry{
		{ID: "a", Addr: "10.0.0.1:6379@16379"},
		{ID: "b", Addr: "10.0.0.2:6379@16379"},
	}
	if e := FindByIP(entries, "10.0.0.2"); e == nil || e.ID != "b" {
		t.Errorf("expected to find b by ip, got %+v", e)
	}
	if e := FindByIP(entries, "10.0.0.9"); e != nil {
		t.Errorf("expected nil for unknown ip, got %+v", e)
	}
	if e := FindByIP(entries, ""); e != nil {
		t.Errorf("expected nil for empty ip, got %+v", e)
	}
}

func TestFindByID(t *testing.T) {
	entries := []Entry{
		{ID: "a", MasterID: "-"},
		{ID: "b", MasterID: "a"},
	}
	if e := FindByID(entries, "b"); e == nil || !e.IsReplica() {
		t.Errorf("expected b as replica, got %+v", e)
	}
	if e := FindByID(entries, "z"); e != nil {
		t.Errorf("expected nil for unknown id, got %+v", e)
	}
}

func TestEntry_HasFlag(t *testing.T) {
	e := Entry{Flags: []string{"myself", "master"}}
	if !e.HasFlag("master") || !e.HasFlag("MYSELF") {
		t.Errorf("expected HasFlag to be case-insensitive, got %+v", e.Flags)
	}
	if e.HasFlag("slave") {
		t.Errorf("expected HasFlag slave=false, got %+v", e.Flags)
	}
}

func TestParseInfo(t *testing.T) {
	raw := "# Server\r\n" +
		"cluster_state:ok\r\n" +
		"cluster_slots_assigned:16384\r\n" +
		"cluster_slots_ok:16384\r\n" +
		"cluster_known_nodes:6\r\n"
	info := ParseInfo(raw)
	if info["cluster_state"] != "ok" {
		t.Errorf("expected cluster_state=ok, got %q", info["cluster_state"])
	}
	if info["cluster_slots_assigned"] != "16384" {
		t.Errorf("expected cluster_slots_assigned=16384, got %q", info["cluster_slots_assigned"])
	}
	if _, ok := info["# Server"]; ok {
		t.Error("expected comment line to be skipped")
	}
}

func TestStateOK(t *testing.T) {
	if !StateOK(map[string]string{"cluster_state": "ok"}) {
		t.Error("expected ok=true for cluster_state=ok")
	}
	if !StateOK(map[string]string{"cluster_state": "OK"}) {
		t.Error("expected ok=true for cluster_state=OK (case-insensitive)")
	}
	if StateOK(map[string]string{"cluster_state": "fail"}) {
		t.Error("expected ok=false for cluster_state=fail")
	}
	if StateOK(map[string]string{}) {
		t.Error("expected ok=false when cluster_state missing")
	}
}

func TestParseSlotSpec_SingleRange(t *testing.T) {
	got, err := ParseSlotSpec("0-8191")
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
	got, err := ParseSlotSpec("0-2,1,5")
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
	if _, err := ParseSlotSpec("0-16384"); err == nil {
		t.Error("expected error for slot 16384")
	}
	if _, err := ParseSlotSpec("-1"); err == nil {
		t.Error("expected error for slot -1")
	}
}

func TestParseSlotSpec_MigratingTokenFails(t *testing.T) {
	if _, err := ParseSlotSpec("[5->-aaa]"); err == nil {
		t.Error("expected error for migrating token in spec")
	}
}

func TestSlotOwnership(t *testing.T) {
	entries := []Entry{
		{ID: "aaa", Slots: []string{"0-100", "5000"}},
		{ID: "bbb", Slots: []string{"101-200"}},
	}
	owner, err := SlotOwnership(entries)
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
	entries := []Entry{
		{ID: "aaa", Slots: []string{"0-100"}},
		{ID: "bbb", Slots: []string{"50"}},
	}
	if _, err := SlotOwnership(entries); err == nil {
		t.Error("expected error when slot owned by two nodes")
	}
}

func TestSlotOwnership_SkipsMigratingTokens(t *testing.T) {
	entries := []Entry{
		{ID: "aaa", Slots: []string{"0-100", "[200->-bbb]"}},
	}
	owner, err := SlotOwnership(entries)
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
	entries := []Entry{
		{ID: "aaa", Slots: []string{"0-100", "[200->-bbb]", "[300-<-ccc]"}},
		{ID: "bbb", Slots: []string{"[500-510->-ddd]"}},
	}
	got := MigratingSlots(entries)
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
	entries := []Entry{
		{ID: "aaa", MasterID: "-", LinkState: "connected"},
		{ID: "bbb", MasterID: "aaa", LinkState: "connected"},
	}
	if !MasterHasHealthyReplica(entries, "aaa") {
		t.Error("expected aaa to have a healthy replica")
	}
}

func TestMasterHasHealthyReplica_FailedReplica(t *testing.T) {
	entries := []Entry{
		{ID: "aaa", MasterID: "-", LinkState: "connected"},
		{ID: "bbb", MasterID: "aaa", Flags: []string{"fail"}, LinkState: "connected"},
	}
	if MasterHasHealthyReplica(entries, "aaa") {
		t.Error("expected aaa to have NO healthy replica when its only replica is failed")
	}
}

func TestMasterHasHealthyReplica_Disconnected(t *testing.T) {
	entries := []Entry{
		{ID: "aaa", MasterID: "-", LinkState: "connected"},
		{ID: "bbb", MasterID: "aaa", LinkState: "disconnected"},
	}
	if MasterHasHealthyReplica(entries, "aaa") {
		t.Error("expected aaa to have NO healthy replica when its replica is disconnected")
	}
}

func TestEntry_Healthy(t *testing.T) {
	if !(Entry{MasterID: "-", LinkState: "connected"}).Healthy() {
		t.Error("expected connected master healthy")
	}
	if (Entry{MasterID: "-", Flags: []string{"fail"}, LinkState: "connected"}).Healthy() {
		t.Error("expected failed node not healthy")
	}
	if (Entry{MasterID: "-", LinkState: "disconnected"}).Healthy() {
		t.Error("expected disconnected node not healthy")
	}
}
