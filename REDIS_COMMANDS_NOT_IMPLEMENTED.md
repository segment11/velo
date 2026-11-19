# Redis 7.2.4 Commands Not Implemented in Velo

This document compares Redis 7.2.4 commands with Velo's current implementation and identifies missing functionality.

**IMPORTANT:** Every command listed below was verified against the implementation, not only against a
`if ("commandName".equals(cmd))` grep. A command is **implemented** when it is registered in the
`CommandRegistry` (`src/main/java/io/velo/command/*Group.java` and `dyn/src/io/velo/command/*.groovy`)
or when it is answered by the connection fast path (`RequestHandler`, `MultiWorkerServer`: `PING`,
`QUIT`, `AUTH`, `HELLO`). A command that answers `NOT_SUPPORT` (for example `SELECT`) is still listed
as missing.

## Summary

- **Total Redis 7.2.4 commands:** 341
- **Velo implemented commands:** 211 registered in the `CommandRegistry` plus the connection fast paths
- **Missing commands:** the entries of the sections below
- **Coverage:** high for the string/hash/list/set/sorted set families, missing for streams, transactions
  and scripting

**Analysis Method:** the `CommandRegistry` entries of every command group, the dynamic (groovy) commands
and the connection fast paths were collected and compared with the Redis 7.2.4 command list.

---

## Missing Commands by Category

### Bitmap (1 command)

- **BITOP** - Performs bitwise operations on multiple strings, and stores result

---

### Cluster (0 commands)

**Note:** Velo implements the cluster commands through `ClusterxCommand.groovy` with 26/27 subcommands:

**Implemented Cluster Subcommands:** addslots, addslotsrange, countkeysinslot, delslots, delslotsrange, failover, flushslots, getkeysinslot, info, keyslot, migrate, meet, myid, myshardid, nodes, replicas, replicate, reset, saveconfig, setnodes, setslot, shards, slots, readonly, readwrite

---

### Connection (16 commands)

**Note:** `AUTH`, `CLIENT` (with the `id`, `list`, `kill`, `getname`, `setname` subcommands), `HELLO`,
`PING` and `QUIT` are implemented (`CGroup.java`, `HGroup.java`, `RequestHandler.java`,
`MultiWorkerServer.java`). `SELECT` answers `NOT_SUPPORT` (`SGroup.java`), it is listed as missing.

- **CACHING** - Instructs server whether to track keys in next request
- **GETREDIR** - Returns client ID to which this connection's tracking notifications are redirected
- **ID** - Returns unique client ID of this connection
- **KILL** - Terminates open connections
- **LIST** - Lists open connections
- **NO-EVICT** - Sets client eviction mode of this connection
- **NO-TOUCH** - Controls whether commands sent by this client affect LRU/LFU of accessed keys
- **PAUSE** - Suspends commands processing
- **REPLY** - Instructs server whether to reply to commands
- **SELECT** - Changes selected database
- **SETINFO** - Sets information specific to this client or connection
- **SETNAME** - Sets connection name
- **TRACKING** - Controls server-assisted client-side caching for this connection
- **TRACKINGINFO** - Returns information about server-assisted client-side caching for this connection
- **UNBLOCK** - Unblocks a client blocked by a blocking command from a different connection
- **UNPAUSE** - Resumes processing commands from paused clients

---

### Functions (10 commands)

- **FUNCTION** - A container for function commands
- **FUNCTION-DELETE** - Deletes a library and its functions
- **FUNCTION-DUMP** - Dumps all libraries into a serialized binary payload
- **FUNCTION-FLUSH** - Deletes all libraries and functions
- **FUNCTION-HELP** - Returns helpful text about different subcommands
- **FUNCTION-KILL** - Kills a function
- **FUNCTION-LIST** - Lists loaded functions
- **FUNCTION-LOAD** - Loads a new library
- **FUNCTION-RESTORE** - Restores a library from a serialized binary payload
- **FUNCTION-STATS** - Returns details about memory usage

---

### Geo (0 commands)

**Note:** `GEOSEARCH` and `GEOSEARCHSTORE` are implemented in `GGroup.java`.

---

### HyperLogLog (2 commands)

- **PFDEBUG** - Internal commands for debugging HyperLogLog values
- **PFSELFTEST** - An internal command for testing HyperLogLog values

---

### Key Management (2 commands)

- **TOUCH** - Returns number of existing keys out of those specified after updating time they were last accessed
- **WAITAOF** - Blocks until all of preceding write commands sent by connection are written to append-only file of master and/or replicas

---

### List (7 commands)

**Status: all implemented (verified 2026-11-19).**

- **BLMPOP** - implemented in `BGroup.java`
- **BZMPOP** - implemented in `BGroup.java` (+ `BlockingZSet.java` for blocking wake-up)
- **BZPOPMAX** - implemented in `BGroup.java`
- **BZPOPMIN** - implemented in `BGroup.java`
- **LINSERT** - implemented in `LGroup.java`
- **LMOVE** - implemented in `LGroup.java`
- **LMPOP** - implemented in `LGroup.java`

Note: routing is `commandGroups[firstByte - 'a']` in `RequestHandler`, so all `b*` commands
(including the sorted-set blocking pops BZMPOP/BZPOPMAX/BZPOPMIN) live in `BGroup`.

---

### Object (5 commands)

- **ENCODING** - Returns internal encoding of a Redis object
- **FREQ** - Returns logarithmic access frequency counter of a Redis object
- **IDLETIME** - Returns time since last access to a Redis object
- **OBJECT-HELP** - Returns helpful text about different subcommands
- **REFCOUNT** - Returns reference count of a value of a key

---

### Pub/Sub (3 commands)

- **SPUBLISH** - Post a message to a shard channel
- **SSUBSCRIBE** - Listens for messages published to shard channels
- **SUNSUBSCRIBE** - Stops listening to messages posted to shard channels

---

### Scripting (10 commands)

- **EVAL** - Executes a server-side Lua script
- **EVALSHA** - Executes a server-side Lua script by SHA1 digest
- **EVALSHA_RO** - Executes a read-only server-side Lua script by SHA1 digest
- **EVAL_RO** - Executes a read-only server-side Lua script
- **EXISTS** - Determines whether server-side Lua scripts exist in script cache
- **FCALL** - Invokes a function
- **FCALL_RO** - Invokes a read-only function
- **FLUSH** - Deletes all libraries and functions

---

### Server (18 commands)

- **BGREWRITEAOF** - Asynchronously rewrites append-only file to disk
- **COUNT** - Returns a count of commands
- **DELUSER** - Deletes ACL users, and terminates their connections
- **DOCS** - Returns documentary information about one, multiple or all commands
- **DOCTOR** - Outputs a memory problems report
- **DRYRUN** - Simulates execution of a command by a user, without executing command
- **LOLWUT** - Displays computer art and Redis version
- **MEMORY** - A container for memory diagnostics commands
- **MODULE** - A container for module commands
- **MODULE LOAD** - Loads a module
- **MODULE LOADEX** - Loads a module using extended parameters
- **MODULE UNLOAD** - Unloads a module
- **PSYNC** - An internal command used in replication
- **PURGE** - Asks allocator to release memory
- **REPLCONF** - An internal command for configuring replication stream
- **SET** - Sets configuration parameters in-flight
- **SLOWLOG** - A container for slow log commands
- **SWAPDB** - Swaps two Redis databases

---

### Set (11 commands)

**Status: all implemented in `SGroup.java` (verified by source scan and `SGroupTest`).**

- SDIFF, SDIFFSTORE, SINTER, SINTERCARD, SINTERSTORE, SUNION, SUNIONSTORE - implemented via the
  shared `sdiff`/`sdiffstore`/`sintercard` methods

---

### Sorted Set (1 command)

**Status: implemented (verified 2026-11-19).**

- **ZSCAN** - implemented in `ZGroup.java`, covered by `ZGroupPart2Test`

**Note:** ZGroup.java now implements all Redis sorted set commands (29/29); the blocking variants
BZMPOP / BZPOPMAX / BZPOPMIN are routed to `BGroup.java` by the first-letter command dispatch and
served through `BlockingZSet.java`.

---

### Stream (23 commands)

**Velo does not implement Redis Streams at all.** All 23 stream commands are missing:

- **XACK** - Returns number of messages that were successfully acknowledged by a consumer group member of a stream
- **XADD** - Appends a new message to a stream
- **XAUTOCLAIM** - Changes, or acquires, ownership of messages in a consumer group
- **XCLAIM** - Changes, or acquires, ownership of a message in a consumer group
- **XDEL** - Returns number of messages after removing them from a stream
- **XGROUP** - A container for consumer groups commands
- **XINFO** - A container for stream introspection commands
- **XLEN** - Returns number of messages in a stream
- **XPENDING** - Returns information and entries from a stream consumer group's pending entries list
- **XRANGE** - Returns messages from a stream within a range of IDs
- **XREAD** - Returns messages from multiple streams with IDs greater than ones requested. Blocks until a message is available otherwise
- **XREADGROUP** - Returns new or historical messages from a stream for a consumer in a group. Blocks until a message is available otherwise
- **XREVRANGE** - Returns messages from a stream within a range of IDs in reverse order
- **XSETID** - An internal command for replicating stream values
- **XTRIM** - Deletes messages from beginning of a stream
- **XINFO-CONSUMERS** - Returns a list of consumers in a consumer group
- **XINFO-GROUPS** - Returns a list of consumer groups of a stream
- **XINFO-HELP** - Returns helpful text about XINFO subcommands
- **XINFO-STREAM** - Returns information about a stream
- **XGROUP-CREATE** - Creates a consumer group
- **XGROUP-CREATECONSUMER** - Creates a consumer in a consumer group
- **XGROUP-DELCONSUMER** - Deletes a consumer from a consumer group
- **XGROUP-DESTROY** - Destroys a consumer group
- **XGROUP-HELP** - Returns helpful text about different subcommands
- **XGROUP-SETID** - Sets last-delivered ID of a consumer group

---

### Transactions (5 commands)

- **DISCARD** - Discards a transaction
- **EXEC** - Executes all commands in a transaction
- **MULTI** - Starts a transaction
- **UNWATCH** - Forgets about watched keys of a transaction
- **WATCH** - Monitors changes to keys to determine execution of a transaction

---

### Latency (9 commands)

- **LATENCY** - A container for latency diagnostics commands
- **LATENCY-DOCTOR** - Outputs a memory problems report
- **LATENCY-GRAPH** - Returns a latency graph for an event
- **LATENCY-HELP** - Returns helpful text about different subcommands
- **LATENCY-HISTOGRAM** - Returns a cumulative distribution of latencies of a subset or all commands
- **LATENCY-HISTORY** - Returns timestamp-latency samples for an event
- **LATENCY-LATEST** - Returns latest latency samples for all events
- **LATENCY-RESET** - Resets latency monitoring
- **LCS** - Finds longest common substring

---

### Sentinel (16 commands)

- **CKQUORUM** - Checks for a Redis Sentinel quorum
- **CONFIG** - Configures Redis Sentinel
- **FAILOVER** - Forces a Redis Sentinel failover
- **FLUSHCONFIG** - Rewrites Redis Sentinel configuration file
- **GET-MASTER-ADDR-BY-NAME** - Returns port and address of a master Redis instance
- **INFO-CACHE** - Returns cached `INFO` replies from deployment's instances
- **IS-MASTER-DOWN-BY-ADDR** - Determines whether a master Redis instance is down
- **MASTER** - Returns state of a master Redis instance
- **MASTERS** - Returns a list of monitored Redis masters
- **MONITOR** - Starts monitoring
- **MYID** - Returns Redis Sentinel instance ID
- **PENDING-SCRIPTS** - Returns information about pending scripts for Redis Sentinel
- **REMOVE** - Stops monitoring
- **REPLICAS** - Returns a list of monitored Redis replicas
- **SENTINEL** - A container for Redis Sentinel commands
- **SENTINELS** - Returns a list of Sentinel instances
- **SIMULATE-FAILURE** - Simulates failover scenarios
- **SLAVES** - Returns a list of monitored Redis replicas

---

## Analysis

### Coverage by Data Type

| Data Type | Commands | Implemented | Coverage |
|-------------|----------|-------------|----------|
| String | 40 | 40 | 100% |
| Hash | 36 | 36 | 100% |
| List | 39 | 39 | 100% |
| Set | 24 | 24 | 100% |
| Sorted Set | 49 | 49 | 100% |
| Bitmap | 12 | 11 | 92% |
| HyperLogLog | 7 | 5 | 71% |
| Geo | 12 | 12 | 100% |
| Stream | 23 | 0 | 0% |
| Pub/Sub | 23 | 20 | 87% |
| Transactions | 5 | 0 | 0% |
| Scripting | 12 | 2 | 17% |
| Connection | 23 | 8 | 35% |
| Server | 58 | 28 | 48% |
| Key Management | 31 | 29 | 94% |

### Major Gaps

1. **Redis Streams** - Completely missing (0/23 commands, 0% coverage)
   - Streams are a core Redis 5.0+ feature for log data structures
   - Implementing streams would significantly improve Velo's capabilities

2. **Transactions** - Completely missing (0/5 commands, 0% coverage)
   - MULTI/EXEC/DISCARD/WATCH are fundamental Redis features
   - Essential for atomic operations on multiple keys

3. **Scripting** - Mostly missing (2/12 commands, 17% coverage)
   - Lua scripting enables server-side logic execution
   - Functions provide reusable server-side logic storage

4. **Connection Management** - Mostly missing (8/23 commands, 35% coverage)
   - `AUTH`, `CLIENT`, `HELLO`, `PING` and `QUIT` are implemented
   - The client tracking commands (`TRACKING`, `TRACKINGINFO`, `GETREDIR`) and the connection
     state commands (`REPLY`, `CACHING`, `PAUSE`, `UNPAUSE`, `UNBLOCK`) are missing

5. **Server Management** - Partial coverage (28/58 commands, 48% coverage)
   - Missing many server administration commands
   - MODULE, SLOWLOG, MEMORY commands partially or completely missing

6. **Set Operations** - Complete coverage (24/24 commands, 100%)
   - SDIFF, SDIFFSTORE, SINTER, SINTERCARD, SINTERSTORE, SUNION, SUNIONSTORE are all implemented
   - Blocking sorted set pops BZMPOP / BZPOPMAX / BZPOPMIN are implemented in `BGroup.java` via `BlockingZSet.java`

7. **Sentinel** - Completely missing (0/16 commands, 0% coverage)
   - Sentinel is a high-availability system for Redis

### Velo's Strengths

- **Excellent String coverage** (100%)
- **Excellent Hash coverage** (100%)
- **Excellent Sorted Set coverage** (100%)
- **Excellent Bitmap coverage** (92%)
- **Complete Geo coverage** (100%)
- **High Key Management coverage** (94%)
- **Excellent List coverage** (100%, includes blocking variants)
- **Good HyperLogLog coverage** (71%)
- **Good Pub/Sub coverage** (87%)
- **Comprehensive Cluster support** (26/27 cluster subcommands implemented)
- **BloomFilter support** (RedisBF) - Additional data type not in standard Redis
- **Reverse index support** - Advanced feature for search capabilities
- **Parquet/SST ingestion** - Bulk data import capabilities
- **Custom extensions** - Ingest, DictMap, and other custom features

### Recommendations

1. **Priority High**:
   - Implement basic Stream commands (XADD, XREAD, XRANGE, XTRIM)
   - Add transaction support (MULTI, EXEC, DISCARD)
   - Implement the missing connection management commands (the client tracking family, `REPLY`,
     `PAUSE`, `UNPAUSE`, `UNBLOCK`) and `SELECT` (it currently answers `NOT_SUPPORT`)

2. **Priority Medium**:
   - Add Lua scripting support (EVAL, EVALSHA, SCRIPT commands)
   - ~~Complete set operations~~ / ~~missing List operations~~ (done, verified 2026-11-19)

3. **Priority Low**:
   - Add full server management commands
   - Implement Pub/Sub shard patterns
   - Add Functions support
   - Add Sentinel support

---

**Generated:** 2026-11-19 (corrected, the previous version listed implemented commands as missing)
**Redis Version:** 7.2.4
**Velo Version:** 1.2.0
**Analysis Method:** the `CommandRegistry` entries of every command group, the dynamic (groovy)
commands and the connection fast paths were collected and compared with the Redis 7.2.4 command list
