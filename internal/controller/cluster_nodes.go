package controller

import (
	"strings"
)

// clusterNodeEntry is the parsed form of a single `CLUSTER NODES` line.
//
// Redis CLUSTER NODES format (space-separated fields):
//
//	<id> <ip:port@cport> <flags> <master> <ping-sent> <pong-recv> <config-epoch> <link-state> <slots...>
//
// <flags> is a comma-separated list such as "master", "slave", "myself",
// "fail?", "handshake", "nofailover". <master> is "-" for a master or the
// master's node id for a replica. <slots...> is the trailing token(s) like
// "0-8191 9000 10001-11000".
type clusterNodeEntry struct {
	ID        string
	Addr      string
	Flags     []string
	MasterID  string
	LinkState string
	Slots     []string
}

func (e clusterNodeEntry) isMaster() bool {
	return e.MasterID == "-"
}

func (e clusterNodeEntry) isReplica() bool {
	return e.MasterID != "-"
}

func (e clusterNodeEntry) hasFlag(flag string) bool {
	for _, f := range e.Flags {
		if strings.EqualFold(f, flag) {
			return true
		}
	}
	return false
}

// hasSlots reports whether this node owns any slots. Slot ownership is
// indicated by trailing tokens that look like slot ranges, e.g. "0-8191" or
// "5000". A node with no trailing slot tokens owns nothing.
func (e clusterNodeEntry) hasSlots() bool {
	return len(e.Slots) > 0
}

// isConnected reports whether the cluster bus link to this node is connected.
func (e clusterNodeEntry) isConnected() bool {
	return strings.EqualFold(e.LinkState, "connected")
}

// isHandshake reports whether the node is still in the CLUSTER NODES handshake
// phase (flags contain "handshake" or "noaddr"). Such entries are transient
// and not part of the cluster's persistent topology, so VerifyCluster and the
// slot/replica accounting helpers ignore them.
func (e clusterNodeEntry) isHandshake() bool {
	return e.hasFlag("handshake") || e.hasFlag("noaddr")
}

// healthy reports whether the node is not flagged failed and its link is up.
func (e clusterNodeEntry) healthy() bool {
	if e.isHandshake() {
		return false
	}
	if e.hasFlag("fail") || e.hasFlag("fail?") {
		return false
	}
	return e.isConnected()
}

// parseClusterNodes parses the raw `CLUSTER NODES` output into entries.
// Lines that fail to parse (too few fields) are skipped rather than failing
// the whole call: CLUSTER NODES can include transient handshake entries.
func parseClusterNodes(raw string) []clusterNodeEntry {
	var out []clusterNodeEntry
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		entry := clusterNodeEntry{
			ID:        fields[0],
			Addr:      fields[1],
			Flags:     strings.Split(fields[2], ","),
			MasterID:  fields[3],
			LinkState: fields[7],
		}
		entry.Slots = append([]string{}, fields[8:]...)
		out = append(out, entry)
	}
	return out
}

// findByIP returns the entry whose announced host matches ip, or nil.
// CLUSTER NODES addresses are "ip:port@cport", so matching the prefix up to
// the first colon is sufficient.
func findByIP(entries []clusterNodeEntry, ip string) *clusterNodeEntry {
	if ip == "" {
		return nil
	}
	for i := range entries {
		if strings.HasPrefix(entries[i].Addr, ip+":") {
			return &entries[i]
		}
	}
	return nil
}

// findByID returns the entry whose node id matches id, or nil.
func findByID(entries []clusterNodeEntry, id string) *clusterNodeEntry {
	for i := range entries {
		if entries[i].ID == id {
			return &entries[i]
		}
	}
	return nil
}
