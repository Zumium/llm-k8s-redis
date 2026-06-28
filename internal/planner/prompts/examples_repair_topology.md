Study the repair step ordering below; the Validator enforces it exactly.

### Example - Repair missing replica (observed: 2 shards, redis-2 has no replica -> desired: 2 shards, 1 replica/shard)
Note: repair topology before any spec-driven change. New repair pods start at nextPodOrdinal.
```json
{
  "dslVersion": "redis.ops/v1alpha1",
  "planId": "repair-missing-replica-001",
  "targetGeneration": 1,
  "summary": "Repair missing replica for redis-2",
  "steps": [
    {"id":"e3","action":"EnsureNode","params":{"namespace":"example","pod":"redis-3","image":"redis:7.2","memorySize":"2Gi"}},
    {"id":"w3","action":"WaitNodeReady","params":{"namespace":"example","pod":"redis-3"}},
    {"id":"m3","action":"MeetNode","params":{"namespace":"example","sourcePod":"redis-2","targetPod":"redis-3"}},
    {"id":"r3","action":"ReplicateNode","params":{"namespace":"example","masterPod":"redis-2","replicaPod":"redis-3"}},
    {"id":"v","action":"VerifyCluster","params":{"expectedShards":2,"expectedReplicasPerShard":1,"requireClusterStateOk":true,"requireFullSlotCoverage":true,"requireAllSlotOwnersHaveReplicas":true}}
  ]
}
```
