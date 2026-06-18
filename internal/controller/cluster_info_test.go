package controller

import "testing"

func TestParseClusterInfo(t *testing.T) {
	raw := "# Server\r\n" +
		"cluster_state:ok\r\n" +
		"cluster_slots_assigned:16384\r\n" +
		"cluster_slots_ok:16384\r\n" +
		"cluster_known_nodes:6\r\n"
	info := parseClusterInfo(raw)
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

func TestClusterStateOk(t *testing.T) {
	if !clusterStateOk(map[string]string{"cluster_state": "ok"}) {
		t.Error("expected ok=true for cluster_state=ok")
	}
	if !clusterStateOk(map[string]string{"cluster_state": "OK"}) {
		t.Error("expected ok=true for cluster_state=OK (case-insensitive)")
	}
	if clusterStateOk(map[string]string{"cluster_state": "fail"}) {
		t.Error("expected ok=false for cluster_state=fail")
	}
	if clusterStateOk(map[string]string{}) {
		t.Error("expected ok=false when cluster_state missing")
	}
}
