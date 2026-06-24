RedisCluster name: {{.Name}}
metadata.generation: {{.Generation}}

## Desired spec
shards: {{.Shards}}
replicasPerShard: {{.ReplicasPerShard}}
image: {{.Image}}
memorySize: {{.MemorySize}}

nextPodOrdinal: {{.NextPodOrdinal}}

## Observed state
{{.ObservedNodesTable}}

## Task
Bring the cluster from the observed state to the desired spec. Pick whichever whitelisted action sequence you think is safest; the controller's Validator is the final safety net.
Return only the JSON plan.
