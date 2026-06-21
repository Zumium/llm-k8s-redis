package controller

import (
	"fmt"
	"strconv"
	"strings"
)

const slotRangeBound = 16383

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
	for i := 1; i < len(out); i++ {
		for j := i; j > 0 && out[j-1] > out[j]; j-- {
			out[j-1], out[j] = out[j], out[j-1]
		}
	}
	return out, nil
}

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
