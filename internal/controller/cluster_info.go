package controller

import (
	"strings"
)

// parseClusterInfo parses the raw `CLUSTER INFO` output into a map. Each line
// is `key:value` (with a trailing `\r` on real Redis). Lines that don't
// contain a colon (comments such as `# Server`) are skipped.
func parseClusterInfo(raw string) map[string]string {
	out := map[string]string{}
	for _, line := range strings.Split(raw, "\n") {
		line = strings.TrimSpace(line)
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		idx := strings.Index(line, ":")
		if idx < 0 {
			continue
		}
		key := strings.TrimSpace(line[:idx])
		val := strings.TrimSpace(line[idx+1:])
		out[key] = val
	}
	return out
}

// clusterStateOk reports whether the parsed CLUSTER INFO reports
// `cluster_state:ok`.
func clusterStateOk(info map[string]string) bool {
	return strings.EqualFold(info["cluster_state"], "ok")
}
