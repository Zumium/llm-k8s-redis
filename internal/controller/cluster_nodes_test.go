package controller

import (
	"testing"
)

func TestParseClusterNodes_MasterAndReplica(t *testing.T) {
	raw := "aaa 10.0.0.1:6379@16379 master - 0 0 1 connected 0-8191\n" +
		"bbb 10.0.0.2:6379@16379 slave aaa 0 0 2 connected\n" +
		"\n"
	entries := parseClusterNodes(raw)
	if len(entries) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(entries))
	}
	m := entries[0]
	if m.ID != "aaa" || !m.isMaster() || m.isReplica() || !m.hasSlots() {
		t.Errorf("unexpected master entry %+v", m)
	}
	if len(m.Slots) != 1 || m.Slots[0] != "0-8191" {
		t.Errorf("expected slots [0-8191], got %v", m.Slots)
	}
	r := entries[1]
	if r.ID != "bbb" || r.isMaster() || !r.isReplica() || r.MasterID != "aaa" || r.hasSlots() {
		t.Errorf("unexpected replica entry %+v", r)
	}
}

func TestParseClusterNodes_SkipsMalformedLines(t *testing.T) {
	raw := "short line\n" +
		"aaa 10.0.0.1:6379@16379 master - 0 0 1 connected\n" +
		"handshake\n"
	entries := parseClusterNodes(raw)
	if len(entries) != 1 {
		t.Fatalf("expected 1 valid entry, got %d", len(entries))
	}
	if entries[0].ID != "aaa" {
		t.Errorf("expected aaa, got %s", entries[0].ID)
	}
}

func TestFindByIP(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "a", Addr: "10.0.0.1:6379@16379"},
		{ID: "b", Addr: "10.0.0.2:6379@16379"},
	}
	if e := findByIP(entries, "10.0.0.2"); e == nil || e.ID != "b" {
		t.Errorf("expected to find b by ip, got %+v", e)
	}
	if e := findByIP(entries, "10.0.0.9"); e != nil {
		t.Errorf("expected nil for unknown ip, got %+v", e)
	}
	if e := findByIP(entries, ""); e != nil {
		t.Errorf("expected nil for empty ip, got %+v", e)
	}
}

func TestFindByID(t *testing.T) {
	entries := []clusterNodeEntry{
		{ID: "a", MasterID: "-"},
		{ID: "b", MasterID: "a"},
	}
	if e := findByID(entries, "b"); e == nil || !e.isReplica() {
		t.Errorf("expected b as replica, got %+v", e)
	}
	if e := findByID(entries, "z"); e != nil {
		t.Errorf("expected nil for unknown id, got %+v", e)
	}
}

func TestClusterNodeEntry_hasFlag(t *testing.T) {
	e := clusterNodeEntry{Flags: []string{"myself", "master"}}
	if !e.hasFlag("master") || !e.hasFlag("MYSELF") {
		t.Errorf("expected hasFlag to be case-insensitive, got %+v", e.Flags)
	}
	if e.hasFlag("slave") {
		t.Errorf("expected hasFlag slave=false, got %+v", e.Flags)
	}
}
