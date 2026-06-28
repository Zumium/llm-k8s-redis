Study the ghost cleanup ordering below; the Validator enforces it exactly.

### Example - Cleanup ghost node (observed: redis-4 is still in CLUSTER NODES but its Pod is gone)
Note: when the Pod is gone, ForgetNode carries lastKnownNodeId from observed state before DeleteNode.
```json
{
  "dslVersion": "redis.ops/v1alpha1",
  "planId": "cleanup-ghost-001",
  "targetGeneration": 1,
  "summary": "Forget and delete dead ghost node redis-4",
  "steps": [
    {"id":"f4","action":"ForgetNode","params":{"namespace":"example","pod":"redis-4","lastKnownNodeId":"node-4"}},
    {"id":"d4","action":"DeleteNode","params":{"namespace":"example","pod":"redis-4"}},
    {"id":"v","action":"VerifyCluster","params":{"expectedShards":2,"expectedReplicasPerShard":1,"requireClusterStateOk":true,"requireFullSlotCoverage":true,"requireAllSlotOwnersHaveReplicas":true}}
  ]
}
```
