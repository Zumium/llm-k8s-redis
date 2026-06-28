Study the dirty node cleanup ordering below; the Validator enforces it exactly.

### Example - Cleanup dirty Pod (observed: redis-4 Pod exists but never joined Redis Cluster)
Note: a Pod with podExists=true and redisSeen=false can be deleted directly because it is not a Redis Cluster member.
```json
{
  "dslVersion": "redis.ops/v1alpha1",
  "planId": "cleanup-dirty-001",
  "targetGeneration": 1,
  "summary": "Delete dirty non-member Pod redis-4",
  "steps": [
    {"id":"d4","action":"DeleteNode","params":{"namespace":"example","pod":"redis-4"}},
    {"id":"v","action":"VerifyCluster","params":{"expectedShards":2,"expectedReplicasPerShard":1,"requireClusterStateOk":true,"requireFullSlotCoverage":true,"requireAllSlotOwnersHaveReplicas":true}}
  ]
}
```
