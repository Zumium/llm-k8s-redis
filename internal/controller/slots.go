package controller

import (
	"fmt"
	"strconv"
	"strings"
)

// slotRangeBound is the inclusive upper bound on a valid Redis Cluster slot.
const slotRangeBound = 16383

// parseSlotSpec parses a slot spec like "0-8191", "0-100,200-300" or "123"
// into a sorted, de-duplicated slice of slot numbers in [0, 16383].
func parseSlotSpec(spec string) ([]int, error) {
	set := map[int]struct{}{}
	for _, part := range strings.Split(spec, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		start, end, err := slotTokenRange(part)
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
	// sort
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out, nil
}

// slotTokenRange parses a single slot token "N" or "N-M" into [start, end]
// inclusive. Tokens that contain square brackets (Redis migration/importing
// markers such as "[5->-node]") are rejected: AddSlots does not handle slots
// in migrating/importing state.
func slotTokenRange(token string) (int, int, error) {
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
		if err := validateSlotBounds(start, end); err != nil {
			return 0, 0, err
		}
		return start, end, nil
	}
	n, err := strconv.Atoi(strings.TrimSpace(token))
	if err != nil {
		return 0, 0, fmt.Errorf("invalid slot %q: %w", token, err)
	}
	if err := validateSlotBounds(n, n); err != nil {
		return 0, 0, err
	}
	return n, n, nil
}

func validateSlotBounds(start, end int) error {
	if start < 0 || end > slotRangeBound || start > end {
		return fmt.Errorf("slot range %d-%d out of bounds [0,%d]", start, end, slotRangeBound)
	}
	return nil
}

// slotOwnership scans parsed CLUSTER NODES entries and builds a map from slot
// number to the node id that owns it. Only "normal" slot tokens (singletons
// and ranges) are recorded; migrating/importing tokens (those wrapped in
// square brackets) are ignored here because AddSlots detects them separately
// via slotTokenRange when validating desired slots.
//
// It returns an error if a slot is owned by more than one node (which should
// never happen on a healthy cluster but indicates corruption).
func slotOwnership(entries []clusterNodeEntry) (map[int]string, error) {
	owner := map[int]string{}
	for _, e := range entries {
		for _, tok := range e.Slots {
			if strings.ContainsAny(tok, "[]") {
				continue
			}
			start, end, err := slotTokenRange(tok)
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

// migratingSlots returns the set of slot numbers that appear in migrating or
// importing tokens (e.g. "[5->-aaa]" or "[5<-aaa]") in the given entries.
// AddSlots treats any desired slot that is currently migrating/importing as a
// failure case: such slots must be resolved before ownership can be claimed.
//
// Redis emits these tokens as "<slot>->-<nodeid>" (migrating away) and
// "<slot>-<-<nodeid>" (importing). The slot itself sits before the first
// angle bracket, with a trailing '-' that belongs to the separator. We strip
// that trailing '-' before parsing the slot.
func migratingSlots(entries []clusterNodeEntry) map[int]struct{} {
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
			start, end, err := slotTokenRange(slotStr)
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
