package controller

import (
	"strings"
)

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

func (e clusterNodeEntry) hasSlots() bool {
	return len(e.Slots) > 0
}

func (e clusterNodeEntry) isConnected() bool {
	return strings.EqualFold(e.LinkState, "connected")
}

func (e clusterNodeEntry) isHandshake() bool {
	return e.hasFlag("handshake") || e.hasFlag("noaddr")
}

func (e clusterNodeEntry) healthy() bool {
	if e.isHandshake() {
		return false
	}
	if e.hasFlag("fail") || e.hasFlag("fail?") {
		return false
	}
	return e.isConnected()
}

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

func findByID(entries []clusterNodeEntry, id string) *clusterNodeEntry {
	for i := range entries {
		if entries[i].ID == id {
			return &entries[i]
		}
	}
	return nil
}
