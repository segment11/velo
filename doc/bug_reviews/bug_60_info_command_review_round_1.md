# Bug 60 Info Command Review Round 1

Author: AI agent 1
Scope: `dyn/src/io/velo/command/InfoCommand.groovy` (the `INFO` command), with supporting context in
`src/main/java/io/velo/persist/OneSlot.java`, `src/main/java/io/velo/MultiWorkerServer.java`,
and `dyn/test/io/velo/command/InfoCommandTest.groovy`.
Status: AI agent 2 reviewed - all findings confirmed

## Module Overview

`InfoCommand` implements the Redis `INFO [section]` command. It is a dynamically loaded Groovy command
(`@CompileStatic`) dispatched on the first slot worker thread (`parseSlots` returns `TO_FIX_FIRST_SLOT`).

The handler supports three arities:

| `data.length` | Behavior |
|---------------|----------|
| `== 2` (`INFO <section>`) | Render exactly one section (`getOneSection`) |
| `== 1` (`INFO`) | Render the default sections + `keyspace` |
| `> 2` (`INFO <s1> <s2> ...`) | Render the requested sections, attach `keyspace` if requested |

Per-section renderers: `server()`, `clients()`, `memory()`, `replication()`, `cpu()`, `cluster()`, and the
async `keyspace()` (aggregates counts across all slots via `localPersist.doSthInSlots`).

This review focused on section routing/edge cases, replication output correctness (Redis Sentinel
compatibility), and the JVM metric arithmetic in `memory()`.

Note: one initially suspicious spot was ruled out and is recorded in "Non-findings" so the reviewer does not
waste time on it.

---

## Finding 1: Unknown / empty INFO section crashes or returns a bogus placeholder

Severity: Medium

Files:
- `dyn/src/io/velo/command/InfoCommand.groovy:61-82` (`getOneSection`, the final `else` branch)
- `dyn/src/io/velo/command/InfoCommand.groovy:86-87` (single-section arity that reaches it)

Code excerpt:

```groovy
private StringOrReply getOneSection(String section) {
    if ('server' == section) {
        ...
    } else if ('keyspace' == section) {
        return new StringOrReply(keyspace())
    } else {
        def r = """# ${section[0].toUpperCase() + section.substring(1)}
key:value
"""
        return new StringOrReply(r.toString())
    }
}
```

Root cause:

The fallback `else` branch is debug/placeholder code left in production. It has two distinct defects:

1. **Empty section crashes.** `INFO ""` (an empty-string section argument) does not match any known section,
   so it falls into `else`. `section[0]` on an empty Groovy `String` throws
   `StringIndexOutOfBoundsException` (and `"".substring(1)` throws as well). The exception is uncaught inside
   `handle()`, so it propagates out of command handling instead of producing a valid INFO reply.

2. **Unknown section returns fake data.** For any unrecognized section (e.g. `INFO foo`, `INFO stats`,
   `INFO all`, `INFO default`), Redis returns the "default" content (all sections). Velo instead returns a
   literal bogus section:

   ```text
   # Foo
   key:value
   ```

   The placeholder `key:value` is meaningless and is served verbatim to any client/monitoring tool that asks
   for a section Redis actually supports (such as `all`, `default`, `stats`, `commandstats`). This is a
   Redis-compatibility regression, not just cosmetic output.

Impact:

- `INFO ""` is a client-triggerable unhandled exception (denial-of-service-flavored: the command errors out
  instead of returning an INFO reply).
- Monitoring/inspection tools that query `INFO all`, `INFO default`, or `INFO stats` receive a fake
  `key:value` section and miss the real statistics they expect.
- The existing test actually *codifies* the broken behavior rather than catching it: the `INFO zzz` case at
  `dyn/test/io/velo/command/InfoCommandTest.groovy:65-71` asserts only that a `BulkReply` comes back, which
  passes for the bogus placeholder. No test asserts correct default/all behavior, and no test exercises the
  empty-string section.

Suggested fix:

- Guard the empty-string case explicitly (return the default/all sections, or at minimum an empty-but-valid
  section) so `section[0]` / `substring(1)` are never reached for `""`.
- Replace the `key:value` placeholder with real Redis semantics: unrecognized sections should map to the
  default/all view (mirroring `INFO` with no argument), not a fake single line.
- Add tests for `INFO ""`, `INFO all`, `INFO default`, and at least one arbitrary unknown section asserting
  that no `key:value` literal leaks out and that no exception is thrown.

AI agent 2 review status: **Confirmed**

Verification notes:

- Current `InfoCommand.getOneSection()` still falls through to the placeholder branch at
  `dyn/src/io/velo/command/InfoCommand.groovy:76-80`.
- The fallback still indexes `section[0]` and calls `section.substring(1)`, so an empty section reaches an
  unguarded string-index path.
- The existing `INFO zzz` test at `dyn/test/io/velo/command/InfoCommandTest.groovy:65-71` only checks that a
  `BulkReply` is returned and does not reject the bogus `key:value` payload.

---

## Finding 2: `connected_slaves` count can disagree with the emitted `slaveN` lines (cap at 2)

Severity: Medium

Files:
- `dyn/src/io/velo/command/InfoCommand.groovy:218-221` (`connected_slaves` uses the full replica list)
- `dyn/src/io/velo/command/InfoCommand.groovy:263-270` (the per-replica `slaveN` loop is capped at `i < 2`)

Code excerpt:

```groovy
def slaveReplPairList = firstOneSlot.slaveReplPairListSelfAsMaster
list << new Tuple2('connected_slaves', slaveReplPairList.size())
...
def replPairAsMasterList = firstOneSlot.replPairAsMasterList
if (!replPairAsMasterList.isEmpty()) {
    // Real Redis emits all slaveN: lines consecutively ...
    for (int i = 0; i < replPairAsMasterList.size() && i < 2; i++) {
        list.addAll slaveConnectState(replPairAsMasterList.get(i), i)
    }
    ...
}
```

Root cause:

`connected_slaves` is set to `slaveReplPairListSelfAsMaster.size()` — the true number of replicas — but the
`slaveN:` line loop is hard-capped with `&& i < 2`. With three or more replicas attached, the INFO
`# Replication` block reports, for example, `connected_slaves:3` while only emitting `slave0` and `slave1`
lines. `slave2` is silently dropped.

The two getters used here (`getSlaveReplPairListSelfAsMaster()` and `getReplPairAsMasterList()` in
`src/main/java/io/velo/persist/OneSlot.java:361-373` and `:513-527`) apply identical filters
(`!isSendBye()` and `isAsMaster()`), so they return the same set of replicas; the inconsistency is purely the
`i < 2` cap, not a filtering mismatch.

Impact:

- Redis Sentinel parses `connected_slaves` and then expects one `slaveN` line per connected replica. A missing
  `slave2` makes that replica invisible to Sentinel, which can cause incorrect replica selection and
  failover decisions.
- The reported `connected_slaves` is an integer that no longer matches the number of `slaveN` lines, which
  violates the invariant real Redis INFO maintains.

Note: `master_replid2` / `second_repl_offset` are Redis concepts for the *post-failover second master id*,
not for a "second slave", so they do not justify capping replica lines at 2.

Suggested fix:

Drop the `&& i < 2` cap and emit one `slaveN` line for every entry in `replPairAsMasterList`, matching the
count already reported by `connected_slaves`. Add a test with 3+ replicas asserting
`connected_slaves == N` and that `slave0..slave{N-1}` are all present.

AI agent 2 review status: **Confirmed**

Verification notes:

- `connected_slaves` is still populated from `firstOneSlot.slaveReplPairListSelfAsMaster.size()` at
  `dyn/src/io/velo/command/InfoCommand.groovy:219-220`.
- The emitted `slaveN` loop still uses `i < replPairAsMasterList.size() && i < 2` at
  `dyn/src/io/velo/command/InfoCommand.groovy:268-270`, so `slave2+` lines are omitted.
- `OneSlot.getSlaveReplPairListSelfAsMaster()` and `OneSlot.getReplPairAsMasterList()` both filter active
  master-side repl pairs by excluding `sendBye` pairs and keeping `isAsMaster()` pairs, so the mismatch is the
  INFO loop cap, not a list-filter discrepancy.

---

## Finding 3: `memory()` mixes the JVM `-1` sentinel for non-heap max into peak/maxmemory/percent

Severity: Low

Files:
- `dyn/src/io/velo/command/InfoCommand.groovy:181-199`

Code excerpt:

```groovy
// nonHeapMemoryUsage.max may == -1
def totalMax = heapMemoryUsage.max + nonHeapMemoryUsage.max
def totalMaxHumanReadable = RamUsageEstimator.humanReadableUnits(totalMax).replace(' ', '')

def usedPercent = (totalUsed / totalMax) * 100
...
used_memory_peak:${totalMax}
used_memory_peak_human:${totalMaxHumanReadable}
used_memory_peak_perc:${usedPercent.round(2)}%
...
maxmemory:${totalMax}
maxmemory_human:${totalMaxHumanReadable}
```

Root cause:

The code itself documents that `nonHeapMemoryUsage.max` may be `-1` (the JVM's "undefined" sentinel) yet
still adds it directly into `totalMax`. That single value then drives four downstream fields:
`used_memory_peak`, `used_memory_peak_human`, `used_memory_peak_perc`, and `maxmemory` / `maxmemory_human`.

When non-heap max is `-1` (common), `totalMax` is off by one and `used_memory_peak` / `maxmemory` report a
nonsensical "max" that is really `heapMax - 1`. If heap max is also undefined (`-1`), `totalMax` becomes
negative, producing a negative `used_memory_peak_perc` and a negative "maxmemory".

A literal divide-by-zero is *not* realistically reachable here (it would require `heapMax + nonHeapMax == 0`
exactly, and a real heap max is far larger than 1), so this finding is about incorrect metric values, not a
crash.

Impact:

- `used_memory_peak` / `maxmemory` can be slightly wrong (off by one) or, in degenerate JVM configurations,
  negative — confusing monitoring dashboards that key off these fields.
- `used_memory_peak_perc` can be negative, which is meaningless.

Suggested fix:

Treat a `-1` (undefined) `max` as "unknown" before summing — e.g. use only the defined maximum, or skip the
peak/percent lines when no reliable max is available — rather than propagating the sentinel through the
arithmetic.

AI agent 2 review status: **Confirmed**

Verification notes:

- `InfoCommand.memory()` still documents that `nonHeapMemoryUsage.max` may be `-1` and immediately adds it to
  `heapMemoryUsage.max` at `dyn/src/io/velo/command/InfoCommand.groovy:181-182`.
- The derived `totalMax` still feeds `used_memory_peak`, `used_memory_peak_human`,
  `used_memory_peak_perc`, `maxmemory`, and `maxmemory_human` at
  `dyn/src/io/velo/command/InfoCommand.groovy:197-203`.
- No guard currently excludes negative JVM sentinel values before computing `usedPercent`.

---

## Non-findings (investigated and ruled out)

### `server()` does NOT leak into the shared static list

`dyn/src/io/velo/command/InfoCommand.groovy:129-148` reads
`MultiWorkerServer.STATIC_GLOBAL_V.infoServerList` and then appends `uptime_in_seconds` /
`uptime_in_days` to it. At first glance this looks like it mutates a shared static list on every call
(unbounded growth). It does **not**: the field is `private final` in
`src/main/java/io/velo/MultiWorkerServer.java:1795` and is only ever exposed through the getter
`getInfoServerList()` at `src/main/java/io/velo/MultiWorkerServer.java:1802-1804`, which returns a fresh copy.
Under Groovy property access, `STATIC_GLOBAL_V.infoServerList` resolves to that getter, so the `// a copy one`
comment is accurate and the appends affect only a local copy. No bug.

### `keyspace()` average-TTL division

`dyn/src/io/velo/command/InfoCommand.groovy:352` divides by `resultList.size()`. `doSthInSlots`
(`src/main/java/io/velo/persist/LocalPersist.java:96-122`) iterates every slot, and the slot count is always
`>= 1`, so the list is never empty in practice; `getAvgTtlInSecond()` returns a primitive `double`, so there is
no null dereference either. Not a bug under normal operation.

---

## Summary

| # | Finding | Severity | AI agent 2 status | Impact |
|---|---------|----------|-------------------|--------|
| 1 | Unknown/empty INFO section crashes or returns bogus `key:value` placeholder | Medium | Confirmed | `INFO ""` throws; `INFO all/default/stats/...` returns fake data instead of real sections |
| 2 | `connected_slaves` count disagrees with emitted `slaveN` lines (capped at 2) | Medium | Confirmed | >2 replicas → Sentinel cannot see `slave2+`; count/lines invariant broken |
| 3 | `memory()` mixes JVM `-1` non-heap max into peak/maxmemory/percent | Low | Confirmed | Slightly wrong or negative `used_memory_peak` / `maxmemory` / `used_memory_peak_perc` |
