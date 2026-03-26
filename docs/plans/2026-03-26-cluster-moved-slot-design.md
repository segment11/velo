# Cluster MOVED Slot E2E Design

**Problem**

Velo maps Redis cluster client slots to inner Velo slots. A redirection reply must expose the Redis client slot, not the inner slot, or Redis clients will be redirected with invalid cluster metadata.

**Test Shape**

Use a black-box e2e test with [`VeloServer`](/home/kerry/ws/velo/src/main/groovy/io/velo/test/tools/VeloServer.groovy). Start one server in `clusterEnabled=true` mode, prewrite `persist/nodes.json`, and define two shard ranges:

- local shard: `0-8191`
- remote shard: `8192-16383`

Choose a hash-tagged key whose Redis CRC16 slot lands in the remote range. Send a raw RESP `GET` to the local Velo instance and assert the reply is:

```text
-MOVED <redis-client-slot> 127.0.0.1:<remote-port>
```

**Why This Design**

- It exercises the real startup path, including `MultiShard` metadata loading.
- It verifies the public wire behavior directly, without depending on internal helpers.
- It avoids a second live node because the bug is in the slot number inside the `MOVED` reply, not in the downstream target’s availability.
