package plan

import "strings"

func ObservedNodeHealthy(n ObservedNode) bool {
	return n.Ready && !n.Deleting && !observedHasFlag(n.Flags, "fail", "fail?", "handshake", "noaddr") && (n.LinkState == "" || strings.EqualFold(n.LinkState, "connected"))
}

func ObservedNodeForgettableGhost(n ObservedNode) bool {
	return n.RedisSeen && n.Slots == "" && (!n.PodExists || n.Deleting || observedHasFlag(n.Flags, "fail", "fail?", "handshake", "noaddr") || strings.EqualFold(n.LinkState, "disconnected"))
}

func observedHasFlag(flags []string, want ...string) bool {
	for _, f := range flags {
		for _, w := range want {
			if strings.EqualFold(f, w) {
				return true
			}
		}
	}
	return false
}
