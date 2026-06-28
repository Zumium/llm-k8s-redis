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
