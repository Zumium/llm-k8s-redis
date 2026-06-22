package rediscluster

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
)

const SlotRangeBound = 16383

type Entry struct {
	ID        string
	Addr      string
	Flags     []string
	MasterID  string
	LinkState string
	Slots     []string
}

func (e Entry) IsMaster() bool {
	return e.MasterID == "-"
}

func (e Entry) IsReplica() bool {
	return e.MasterID != "-"
}

func (e Entry) HasFlag(flag string) bool {
	for _, f := range e.Flags {
		if strings.EqualFold(f, flag) {
			return true
		}
	}
	return false
}

func (e Entry) HasSlots() bool {
	return len(e.Slots) > 0
}

func (e Entry) IsConnected() bool {
	return strings.EqualFold(e.LinkState, "connected")
}

func (e Entry) IsHandshake() bool {
	return e.HasFlag("handshake") || e.HasFlag("noaddr")
}

func (e Entry) Healthy() bool {
	if e.IsHandshake() {
		return false
	}
	if e.HasFlag("fail") || e.HasFlag("fail?") {
		return false
	}
	return e.IsConnected()
}

func ParseNodes(raw string) []Entry {
	var out []Entry
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" {
			continue
		}
		fields := strings.Fields(line)
		if len(fields) < 8 {
			continue
		}
		entry := Entry{
			ID:        fields[0],
			Addr:      fields[1],
			Flags:     strings.Split(fields[2], ","),
			MasterID:  fields[3],
			LinkState: fields[7],
			Slots:     append([]string{}, fields[8:]...),
		}
		out = append(out, entry)
	}
	return out
}

func FindByIP(entries []Entry, ip string) *Entry {
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

func FindByID(entries []Entry, id string) *Entry {
	for i := range entries {
		if entries[i].ID == id {
			return &entries[i]
		}
	}
	return nil
}

func ParseInfo(raw string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, ":")
		if !ok {
			continue
		}
		out[strings.TrimSpace(k)] = strings.TrimSpace(v)
	}
	return out
}

func StateOK(info map[string]string) bool {
	return strings.EqualFold(info["cluster_state"], "ok")
}

func ParseSlotSpec(spec string) ([]int, error) {
	set := map[int]struct{}{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		start, end, err := SlotTokenRange(part)
		if err != nil {
			return nil, err
		}
		for s := start; s <= end; s++ {
			set[s] = struct{}{}
		}
	}
	out := make([]int, 0, len(set))
	for s := range set {
		out = append(out, s)
	}
	sort.Ints(out)
	return out, nil
}

func SlotTokenRange(token string) (int, int, error) {
	if strings.ContainsAny(token, "[]") {
		return 0, 0, fmt.Errorf("slot token %q is in a migrating/importing state", token)
	}
	if strings.Contains(token, "-") {
		rg := strings.SplitN(token, "-", 2)
		start, err := strconv.Atoi(strings.TrimSpace(rg[0]))
		if err != nil {
			return 0, 0, fmt.Errorf("invalid slot range %q: %w", token, err)
		}
		end, err := strconv.Atoi(strings.TrimSpace(rg[1]))
		if err != nil {
			return 0, 0, fmt.Errorf("invalid slot range %q: %w", token, err)
		}
		if err := ValidateSlotBounds(start, end); err != nil {
			return 0, 0, err
		}
		return start, end, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(token))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid slot %q: %w", token, err)
	}
	if err := ValidateSlotBounds(n, n); err != nil {
		return 0, 0, err
	}
	return n, n, nil
}

func ValidateSlotBounds(start, end int) error {
	if start < 0 || end > SlotRangeBound || start > end {
		return fmt.Errorf("slot range %d-%d out of bounds [0,%d]", start, end, SlotRangeBound)
	}
	return nil
}

func SlotOwnership(entries []Entry) (map[int]string, error) {
	owner := map[int]string{}
	for _, e := range entries {
		for _, tok := range e.Slots {
			if strings.ContainsAny(tok, "[]") {
				continue
			}
			start, end, err := SlotTokenRange(tok)
			if err != nil {
				continue
			}
			for s := start; s <= end; s++ {
				if prev, ok := owner[s]; ok && prev != e.ID {
					return nil, fmt.Errorf("slot %d owned by both %s and %s", s, prev, e.ID)
				}
				owner[s] = e.ID
			}
		}
	}
	return owner, nil
}

func MigratingSlots(entries []Entry) map[int]struct{} {
	out := map[int]struct{}{}
	for _, e := range entries {
		for _, tok := range e.Slots {
			if !strings.ContainsAny(tok, "[]") {
				continue
			}
			inner := strings.Trim(tok, "[]")
			idx := strings.IndexAny(inner, "<>")
			if idx < 0 {
				continue
			}
			slotStr := strings.TrimSuffix(strings.TrimSpace(inner[:idx]), "-")
			if slotStr == "" {
				continue
			}
			start, end, err := SlotTokenRange(slotStr)
			if err != nil {
				continue
			}
			for s := start; s <= end; s++ {
				out[s] = struct{}{}
			}
		}
	}
	return out
}

type MigrationMarker struct {
	SourceID string
	TargetID string
}

func SlotMigrationMarkers(entries []Entry) map[int]MigrationMarker {
	out := map[int]MigrationMarker{}
	for _, entry := range entries {
		for _, tok := range entry.Slots {
			if !strings.ContainsAny(tok, "[]") {
				continue
			}
			inner := strings.Trim(tok, "[]")
			if idx := strings.Index(inner, "->-"); idx >= 0 {
				slot, ok := MigrationMarkerSlot(inner[:idx])
				if ok {
					out[slot] = MigrationMarker{SourceID: entry.ID, TargetID: strings.TrimSpace(inner[idx+3:])}
				}
				continue
			}
			if idx := strings.Index(inner, "<-"); idx >= 0 {
				slot, ok := MigrationMarkerSlot(inner[:idx])
				if !ok {
					continue
				}
				m := out[slot]
				m.SourceID = strings.TrimSpace(inner[idx+2:])
				m.TargetID = entry.ID
				out[slot] = m
			}
		}
	}
	return out
}

func MigrationMarkerSlot(raw string) (int, bool) {
	n, err := strconv.Atoi(strings.TrimSuffix(strings.TrimSpace(raw), "-"))
	if err != nil || n < 0 || n > SlotRangeBound {
		return 0, false
	}
	return n, true
}

func MasterHasHealthyReplica(entries []Entry, masterID string) bool {
	for _, e := range entries {
		if e.IsReplica() && e.MasterID == masterID && e.Healthy() {
			return true
		}
	}
	return false
}

func HealthyMasters(entries []Entry) []Entry {
	var out []Entry
	for _, e := range entries {
		if e.IsMaster() && e.Healthy() {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func HealthyReplicasOf(entries []Entry, masterID string) []Entry {
	var out []Entry
	for _, e := range entries {
		if e.IsReplica() && e.MasterID == masterID && e.Healthy() {
			out = append(out, e)
		}
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

func IPFromAddr(addr string) string {
	if i := strings.Index(addr, ":"); i >= 0 {
		return addr[:i]
	}
	return addr
}

func RedisAddrFromClusterAddr(addr string) string {
	if i := strings.Index(addr, "@"); i >= 0 {
		return addr[:i]
	}
	return addr
}

func RedisHostPortFromAddr(addr string) (string, int) {
	addr = RedisAddrFromClusterAddr(addr)
	host := addr
	port := 6379
	if i := strings.LastIndex(addr, ":"); i >= 0 {
		host = addr[:i]
		if n, err := strconv.Atoi(addr[i+1:]); err == nil && n > 0 {
			port = n
		}
	}
	if host == "" {
		host = "127.0.0.1"
	}
	return host, port
}
