package plan

import "testing"

func TestObservedNodeHealthy(t *testing.T) {
	base := ObservedNode{Ready: true, PodExists: true, RedisSeen: true, LinkState: "connected"}
	cases := []struct {
		name string
		edit func(*ObservedNode)
		want bool
	}{
		{name: "ready connected", want: true},
		{name: "fail", edit: func(n *ObservedNode) { n.Flags = []string{"fail"} }},
		{name: "fail?", edit: func(n *ObservedNode) { n.Flags = []string{"fail?"} }},
		{name: "handshake", edit: func(n *ObservedNode) { n.Flags = []string{"handshake"} }},
		{name: "noaddr", edit: func(n *ObservedNode) { n.Flags = []string{"noaddr"} }},
		{name: "disconnected", edit: func(n *ObservedNode) { n.LinkState = "disconnected" }},
		{name: "deleting", edit: func(n *ObservedNode) { n.Deleting = true }},
		{name: "not ready", edit: func(n *ObservedNode) { n.Ready = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := base
			if tc.edit != nil {
				tc.edit(&n)
			}
			if got := ObservedNodeHealthy(n); got != tc.want {
				t.Fatalf("ObservedNodeHealthy() = %v, want %v", got, tc.want)
			}
		})
	}
}

func TestObservedNodeForgettableGhost(t *testing.T) {
	base := ObservedNode{PodExists: true, RedisSeen: true, Ready: true}
	cases := []struct {
		name string
		edit func(*ObservedNode)
		want bool
	}{
		{name: "pod gone", edit: func(n *ObservedNode) { n.PodExists = false }, want: true},
		{name: "deleting", edit: func(n *ObservedNode) { n.Deleting = true }, want: true},
		{name: "fail", edit: func(n *ObservedNode) { n.Flags = []string{"fail"} }, want: true},
		{name: "fail?", edit: func(n *ObservedNode) { n.Flags = []string{"fail?"} }, want: true},
		{name: "handshake", edit: func(n *ObservedNode) { n.Flags = []string{"handshake"} }, want: true},
		{name: "noaddr", edit: func(n *ObservedNode) { n.Flags = []string{"noaddr"} }, want: true},
		{name: "disconnected", edit: func(n *ObservedNode) { n.LinkState = "disconnected" }, want: true},
		{name: "slot owner", edit: func(n *ObservedNode) { n.PodExists = false; n.Slots = "0-1" }},
		{name: "not ready only", edit: func(n *ObservedNode) { n.Ready = false }},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			n := base
			if tc.edit != nil {
				tc.edit(&n)
			}
			if got := ObservedNodeForgettableGhost(n); got != tc.want {
				t.Fatalf("ObservedNodeForgettableGhost() = %v, want %v", got, tc.want)
			}
		})
	}
}
