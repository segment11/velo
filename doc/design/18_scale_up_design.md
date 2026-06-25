# Velo Single-Host Scale-Up Design

## Goal

Let a **single running Velo instance grow its data capacity online (no restart)** as data grows,
across all three pressure dimensions:

- **Key count** — too many keys per slot, exhausting `KeyBucket` capacity and its bounded split headroom.
- **Value bytes** — total value volume exhausting chunk segments, even when key count is fine.
- **Throughput / CPU** — more ops/sec than the current slot-worker parallelism can serve.

This is *vertical* scale-up: use more of the same host's RAM / disk / cores. It is distinct from
cross-host scale-out, but it deliberately reuses the cluster routing + migration model.

## Core Idea

**Run the cluster's routing + migration model inside one process** — "a cluster of `OneSlot`s in a single
node." A fixed virtual-slot space stays constant while the number of physical `OneSlot`s grows online. The
pieces already exist ([`MultiShard`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/MultiShard.java),
[`SlotRange`](/home/kerry/ws/velo/src/main/java/io/velo/repl/cluster/SlotRange.java), per-slot
[`Binlog`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Binlog.java), `MGroup` migration); today they are
gated behind `clusterEnabled` and aimed at remote nodes. Scale-up aims the same machinery at local `OneSlot`s.

The catch — and why this is more than "enable cluster mode locally" — is that the single-host path hard-bakes
capacity at startup, and the threading model pins slots to threads. The structural changes below are what
remove those walls.

## Current Model And Why It Caps

A data shard is a per-slot [`OneSlot`](/home/kerry/ws/velo/src/main/java/io/velo/persist/OneSlot.java), and
total capacity is fixed at startup as:

```
capacity ≈ estimateKeyNumber (per slot) × slotNumber
```

Everything that determines capacity is frozen at init and never changes at runtime.

### The slot hash is doubling-hostile

In [`BaseCommand.slot()`](/home/kerry/ws/velo/src/main/java/io/velo/BaseCommand.java) (non-cluster branch,
lines 637-649):

```java
final int halfSlotNumber = slotNumber / 2;
final int x = halfSlotNumber * bucketsPerSlot;
var slotPositive = (keyHash / x) & (halfSlotNumber - 1);
// bucketIndex = keyHash & (bucketsPerSlot - 1)
```

The slot index is a **bit-window whose position and width both depend on `slotNumber`**. Going `N → 2N`
shifts and widens that window, so almost every key remaps — you cannot cleanly split one slot in two. This
formula assumes a fixed `N`. The cluster branch (lines 631-635) instead uses a routing table
(`CRC16 → 16384 client slots → innerSlot via MultiShard`); that indirection is the model to adopt.

The one portable fact: `bucketIndex = keyHash & (bucketsPerSlot - 1)` is **slot-independent** (low bits only),
and `bucketsPerSlot` is global to every slot. A key's bucket position does not change when a different
physical slot takes ownership — this is what makes migration cheap.

## Verified Constraints (the things that make this hard)

Confirmed against the current code; these are the hard edges any design must respect.

| # | Constraint | Evidence | Why it matters |
|---|------------|----------|----------------|
| 1 | `bucketsPerSlot` is one global value shared by every slot | `ConfForSlot.java:254`, `OneSlot.java:227`, `BaseCommand.java:626` | `bucketIndex` is portable across slots **only while every slot shares `bucketsPerSlot`**. A heterogeneous per-slot bucket count would break migration portability. |
| 2 | Slots pinned to worker threads by `slot % slotWorkers`; count fixed at startup; `slotNumber % slotWorkers == 0` enforced | `MultiWorkerServer.java:603,1634-1650`, `ConfForGlobal.java:95`, `LocalPersist.java:178-195` | A new physical slot cannot appear on a running event loop. This is **wall #2**, the real blocker — routing is easy, threading is hard. |
| 3 | Run-to-completion is `@ThreadNeedLocal`; slot data bound to one thread, lock-free hot path | `RequestHandler.java:35`, `OneSlot` | Elasticity must not sacrifice the lock-free invariant. Moving a slot between threads requires rebuilding its thread-local state. |
| 4 | Per-slot `Binlog`; master/slave handshake **rejects** `slotNumber` mismatch | `Binlog.java:20-24`, `OneSlot.java:294`, `ConfForSlot.java:104,160-172` (`slaveCanMatch`, line 165) | **Wall #3.** If a master adds/splits slots online, slaves break unless the reshard itself replicates through the binlog. |
| 5 | `MGroup` migration reads the full logical value via `OneSlot.get()` (WAL → LRU → buckets → chunk → big-string) | `MGroup.java:434-439`, `OneSlot.java:1542-1637` | Migration already captures WAL-delayed and big-string values. A naive bucket-file copy would miss un-flushed writes; reuse `get()`. |
| 6 | Hash-tag `{tag}` keys route by `tagHash` so related keys co-locate; multi-key commands group by slot | `BaseCommand.java:586-612,640-644`, `MGroup.java:124-151` | Virtual-slot remapping must keep a whole tag group on one physical slot, or break atomic multi-key ops. |
| 7 | Chunk segment addressing is static: `fdIndex = segIdx / segmentNumberPerFd`, `offset = segIdx % segmentNumberPerFd`; `maxSegmentIndex` fixed at init | `Chunk.java:102-108,248-271`, `ConfChunk.java:377-378` | Appending chunk fd files at runtime **breaks existing segment addresses** (the modulo assumes fixed `fdPerChunk`). Elastic chunk growth needs an addressing scheme that survives growth, plus metadata migration in `MetaChunkSegmentFlagSeq`/`MetaChunkSegmentIndex`. |
| 8 | `MAX_SLOT_NUMBER = 1024`; `slotNumber` validated once, must be power-of-two; `OneSlot[]` sized once | `LocalPersist.java:53,188`, `MultiWorkerServer.java:1601-1611` | **Wall #1.** The slot array, slot→worker modulo, hash-tag slicing, and slave handshake all assume `slotNumber` is constant. |

## Target Model

- **Fixed virtual-slot space**, defined from the **same `keyHash` high bits** that the bucket index uses for
  its low bits. Then a virtual-slot range corresponds to a **contiguous bucket range**, so a range migration
  is a near-contiguous bucket-range transfer rather than a full per-slot key scan. (This is the refinement
  over blindly reusing the cluster's independent CRC16 client-slot space.)
- **Variable set of physical `OneSlot`s**, added online. A routing table (generalized `MultiShard`, usable
  when `clusterEnabled = false`) maps virtual-slot ranges → physical slot. `bucketIndex` is unchanged by the
  move (constraint #1), so the recipient places keys without rehashing buckets.
- Scaling up = **add a physical `OneSlot`, reassign virtual ranges to it, migrate only those ranges**.

| Pressure | Mechanism |
|----------|-----------|
| Key count | Move ranges off a hot slot → halves its keys → relieves `KeyBucket` split headroom |
| Value bytes | Same move halves chunk volume; plus Phase-0 elastic chunk growth |
| Throughput/CPU | More physical slots, rebalanced across slot-worker threads → more parallelism |

## Centerpiece: Safepoint Reshard

Rather than implement Redis-style `MIGRATING`/`IMPORTING` dual-routing (a write racing a key that is
mid-move), **remove the concurrency for the duration of the structural change** — a GC-safepoint analogy.

> **Quiesce to one slot-worker thread → mutate routing + slot ownership in a single-threaded total order → resume multi-thread.**

Because all affected slots have a single owner thread during the edit, every operation is totally ordered and
the mid-move write race **cannot exist**. The run-to-completion invariant is never violated: each slot always
has exactly one owner thread; we only narrow "N owners" to "1 owner" and back. Given how much recent work is
replication-correctness, trading a brief brownout for "this race class is structurally impossible" is the
right v1 trade.

### Scaling knob: copy-then-cutover vs full-freeze

Single-threading the *entire* data copy is a long brownout (whole instance at `1/slotWorkers` throughput for
the migration duration). So split the work:

1. **Bulk copy** runs concurrently while still multi-threaded (read via `OneSlot.get()`, constraint #5).
2. **Cutover** takes the single-thread safepoint only for the final flip: drain the write tail, swap the
   routing table (`key→slot`) and the slot→worker binding, finalize ownership.

Pick by data volume against a **max-pause budget**: small/rare migrations can full-freeze; large ones must
copy-then-cutover.

### Localized vs global collapse

- **Global collapse** (fold *all* slots onto one thread): simplest to implement; biggest brownout. Good v1.
- **Localized** (fold only donor + recipient slots onto one chosen thread; other slots keep serving at full
  multi-thread speed): better availability; brownout limited to the moving key ranges. Preferred later.

## Threading: Keep the Run-to-Completion Invariant

For v1, **do not add slot-worker threads or rehome slots inside a live process**. Changing the slot-worker
execution topology is a deeper runtime mutation than value-container growth: `OneSlot` ownership is
thread-affine, command execution uses `@ThreadNeedLocal`, and `LocalPersist`/bootstrap currently size worker
bindings at startup. A rolling restart or replica-promotion flow is an acceptable CPU scale-up mechanism and
matches how many storage engines treat deep execution-topology changes.

Reference patterns from other engines:

| Engine | CPU scale-up pattern | Lesson for Velo |
|--------|----------------------|-----------------|
| PostgreSQL | More CPU helps more backend processes; some worker/config limits require restart. | Restart for deeper CPU topology is normal. |
| MySQL/InnoDB | Some concurrency knobs are runtime-tunable; deeper thread/layout assumptions are startup-time. | Runtime knobs tune scheduling, not ownership migration. |
| RocksDB | Background flush/compaction threads can be adjusted online; foreground work is caller-thread driven. | Online CPU scaling is easiest for background work. |
| Redis | Main command execution topology is mostly fixed; CPU scale uses cluster/processes/IO threads. | Local multi-process or rolling restart is the baseline. |
| Elasticsearch/Cassandra | Thread pools exist, but shard/token ownership changes are topology operations. | Treat CPU scale-up as topology/config change. |

So the v1 CPU story should be:

1. Start a replacement instance or promote a replica with a higher `slotWorkers` value.
2. Let replication catch up or use the cluster/failover path to switch traffic.
3. Retire the old lower-worker instance.

Two live in-process alternatives remain possible later, but should not be v1:

- **Option A — abandon run-to-completion** (shared `OneSlot` state + locks). **Rejected.** It regresses the
  steady-state lock-free hot path on every slot to fix a rare growth event.
- **Option B — keep the invariant, make the binding dynamic**. Replace `slot % slotWorkers` with a mutable
  slot→worker table, and add a *rehome* operation: quiesce a slot, drain in-flight, rebuild its
  `@ThreadNeedLocal` state on the target event loop (ActiveJ `execute`/`submit`), flip the table. This is a
  later throughput feature only if rolling restart / replica promotion proves too disruptive.

Adding a physical slot to an existing worker can still be useful for space-oriented resharding, because it does
not require cross-thread ownership movement. Moving a slot between workers is the hard half and is needed only
for the throughput/CPU dimension.

## Difficult Points / Open Hard Problems

These are unresolved and need design decisions before implementation.

1. **`@ThreadNeedLocal` rebuild on rehome.** Exactly what each slot stashes in ThreadLocal must be audited;
   rehoming requires tearing down and rebuilding that state on the new event loop. Sizing and correctness of
   this is the trickiest engineering piece. (Constraint #3.)
2. **Crash during reconfig must be atomic/resumable.** The safepoint edit (routing table + slot ownership +
   chunk/bucket layout) must be journaled so a crash mid-reshard restarts into a consistent topology, not a
   half-migrated one. Needs a durable reconfig journal with idempotent replay.
3. **Replication of the reshard (wall #3).** A topology-change record must be emitted into the binlog so
   slaves replay the identical split/remap, instead of failing the static `slaveCanMatch` slotNumber check
   (constraint #4). Slave replay is single-stream, which fits the safepoint model — but the handshake and
   `SlaveCheckValues` need to express a *variable* physical-slot topology.
4. **In-flight blocking commands during quiesce.** `BLPOP`/`BRPOP`/transactions held on a slot at the
   safepoint must be drained or rejected deterministically; define the policy.
5. **Elastic chunk growth is not free (constraint #7).** Online value-bytes growth within a slot needs a
   segment-addressing scheme that survives appending fd files, plus migration of
   `MetaChunkSegmentFlagSeq`/`MetaChunkSegmentIndex`. The current modulo addressing breaks on growth.
6. **Heterogeneous per-slot capacity vs migration portability (constraint #1).** Portable `bucketIndex`
   holds only while every slot shares `bucketsPerSlot`. If a new physical slot wants a larger bucket table
   (to absorb key-count growth), migration into it stops being a clean bucket-range transfer. Decide whether
   all physical slots stay homogeneous.
7. **Hash-tag group integrity (constraint #6).** Range remapping must never split a `{tag}` group across two
   physical slots; the virtual-slot granularity and range boundaries must respect tag hashing.
8. **Memory pressure under global collapse.** One thread transiently owning all slots' working set (LRUs,
   WAL buffers) may spike memory/GC; the localized-collapse variant mitigates this.
9. **Capacity-planning story changes.** Once physical slots are dynamic, `estimateKeyNumber × slotNumber`
   stops being the sizing model. Operators need new guidance: how to choose initial `bucketsPerSlot`,
   virtual-slot count, and the max-pause budget.

## Reference: How LSM / RocksDB Stays Elastic (ideas to borrow, not decisions)

RocksDB almost never hits a "full, restart with bigger params" wall. Worth understanding why, as a source of
ideas — **nothing here is decided**, and Velo's hash-index, direct-addressing core is deliberately different.

**RocksDB's params are triggers, not allocations.** `write_buffer_size`, `target_file_size_base`,
`max_bytes_for_level_base × max_bytes_for_level_multiplier`, `level0_file_num_compaction_trigger` are *targets
that fire compaction*, not pre-sized capacity. As data grows it just creates more SST files and pushes them
down levels; nothing is sized to the dataset up front.

**The one immutable param (`num_levels`) still does not cap capacity**, because the bottom level (Lmax) is
unbounded — it absorbs unlimited growth. Undersizing it costs **write/space/read amplification**, not a hard
stop. `level_compaction_dynamic_level_bytes` even self-tunes level boundaries from the actual bottom-level size
upward, and most options are runtime-mutable via `SetOptions()`.

**Why RocksDB gets this for free and Velo does not — addressing.**

- RocksDB = **indirect / searched** addressing (index block + bloom filter + binary search). A key has no
  fixed home, so adding files/levels re-addresses nothing. Capacity is elastic; the price is amplification,
  paid down by compaction.
- Velo = **direct** addressing (`bucketIndex = keyHash & (bucketsPerSlot-1)`; segment index → `(fd, offset)`
  by fixed modulo, `Chunk.java:248-271`). O(1), no search — Velo's low-latency selling point — but a fixed
  address space ⇒ fixed capacity ⇒ growth needs repartition.

This is the fundamental trade: **direct addressing buys latency at the cost of fixed capacity; LSM buys
elasticity at the cost of amplification.** The safepoint reshard is the price Velo pays *because* it refuses
the LSM's amplification. So Velo cannot simply "do what RocksDB does" without giving up its core property.

**Ideas Velo could borrow without abandoning the hash index** (candidates only):

1. **Unbounded chunk tail (RocksDB "Lmax is unlimited").** Velo's chunk layer is already LSM-shaped
   (WAL → segments → merge ≈ compaction). One could let it append fd files as an unbounded tail instead of a
   fixed `segmentNumberPerFd × fdPerChunk` ceiling — removing the *value-bytes* wall with no reshard, at the
   cost of some space/read amplification (RocksDB's bargain). Relates to constraint #7 / hard problem #5.
2. **Treat `estimateKeyNumber × slotNumber` as a trigger, not a pre-allocation.** Like
   `max_bytes_for_level_base`: a target that, when exceeded, *fires the safepoint reshard* — the reshard
   becomes Velo's "compaction event." Pair with `SetOptions()`-style runtime-mutable elastic params, while the
   structural topology (the `num_levels` analog) is what a reshard changes.
3. **The key-bucket hash table is the part that cannot be LSM'd** — it is the direct-addressing core
   (constraint #1). This is *why* value-bytes growth could be elastic (idea #1) while key-count growth still
   needs repartition, and throughput still needs rehome — the three pressures fundamentally differ.

## Reference: How Other Hash-Index Engines Scale Up (ideas to borrow, not decisions)

Velo is hash-index / direct-addressing, so LSM stores (RocksDB above) are only a partial analogy. These four
hash-index systems are closer, and each maps to a specific Velo wall. **Nothing here is decided** — they are
prior art for the open concurrency-model question.

### Redis `dict` — incremental rehashing (key-count wall, online, no stop-the-world)

Redis grows its in-memory hash table **without a global pause**. On load-factor ≥ 1 it allocates a second
table `ht[1]` at 2× size and sets `rehashidx`; each subsequent operation moves **one bucket** from `ht[0]` to
`ht[1]` (`_dictRehashStep`). During the migration, reads check both tables, writes go only to `ht[1]`, and the
resize cost is amortized across thousands of commands. Notably, Redis **suspends rehash growth while a
`fork()` (BGSAVE/AOF) is in progress** to avoid touching copy-on-write pages.

- **Maps to:** the key-bucket hash table — Velo's hardest wall (key count), which constraint #1 says cannot be
  LSM'd. Redis shows direct-addressed hash tables *can* grow online via dual-table incremental migration.
- **Why it fits Velo's threads well:** the `@ThreadNeedLocal` single-owner-per-slot model means one thread
  owns a slot's buckets, so a per-operation "move one bucket" step needs **no locking** — arguably a better
  fit than a global safepoint *for the key-count dimension specifically*. The cost is on-disk: a second bucket
  table doubles disk and requires migrating 4KB pages. The fork-suspension lesson maps directly to Velo: an
  incremental rehash must coordinate with binlog/replication and chunk merge, not run blindly.

### Couchbase — vBuckets (validates the virtual-slot model)

Couchbase hashes keys (CRC32) into a **fixed 1024 vBuckets**, decoupled from the variable set of physical
nodes via a routing map. Scaling = **rebalance**: whole vBuckets move between nodes, transferred
**sequentially and resumably**; when a vBucket's data is fully copied, it is **atomically made "active"** on
the new node and traffic cuts over.

- **Maps to:** the doc's entire target model — fixed virtual-slot space + routing table + per-range migration
  + atomic cutover. This is essentially the proposed design, industry-proven.
- **Lessons:** a fixed virtual count (1024) decoupled from physical owners; sequential **resumable** migration
  (≈ hard problem #2's reconfig journal); atomic activate at the end (≈ the safepoint cutover flip). Validates
  direction; does not solve the in-process threading piece (Couchbase nodes are separate processes — the
  Multi-Process alternative below).

### Microsoft FASTER — epoch-protected resizable index (an alternative to the safepoint)

FASTER's in-memory, fixed-size-cell hash index **doubles online without latches** using **epoch protection**
plus a phase state machine: *allocate* the 2× table, *migrate* entries gradually, *switch over* — readers in
an epoch safely use the old table, and old structures are freed only after all threads exit the relevant
epoch. No global stop.

- **Maps to:** the threading/concurrency model (Option A vs B). Epochs are a **third option** between
  "locks everywhere" (Option A, rejected) and "stop-the-world safepoint": grow concurrently with no brownout,
  at the cost of an epoch/phase framework.
- **Trade-off vs safepoint reshard:** FASTER avoids the brownout but adds substantial concurrency machinery
  and is the source of subtle bugs. The safepoint reshard is far simpler to make correct (its whole point);
  FASTER is what you reach for only if the brownout proves unacceptable.

### Bitcask (Riak) — in-memory hash + append-only log + merge (value-bytes wall)

Bitcask keeps an in-memory hash index (`keydir`) pointing into **append-only** data files; one active file for
writes, the rest immutable. Value capacity grows simply by **appending new files**, and a background **merge**
rewrites immutable files to drop stale/tombstoned entries and reclaim space. The hash index must fit in RAM.

- **Maps to:** the value-bytes dimension and the "unbounded chunk tail" idea (#1 in the RocksDB section).
  Velo's chunk layer is already append+merge shaped; Bitcask confirms that elastic value growth is the easy
  dimension — append files, compact in the background — while the *hash index* is the part that stays the
  capacity constraint (same split as Velo: chunk elastic, key-bucket not).

### Summary mapping

| Engine | Online-grows | Mechanism | Velo wall it informs |
|--------|--------------|-----------|----------------------|
| Redis `dict` | the hash table | dual-table incremental rehash, 1 bucket/op | key-count (key buckets) |
| Couchbase | physical placement | fixed vBuckets + resumable rebalance + atomic cutover | the whole virtual-slot/routing model |
| FASTER | the hash index | epoch-protected phased resize, latch-free | the concurrency model (safepoint alternative) |
| Bitcask | value data | append-only files + background merge | value-bytes (elastic chunk tail) |

**Cross-cutting takeaway:** the three pressure dimensions are genuinely different problems, and the prior art
confirms it — value-bytes is elastic by append+merge (Bitcask/RocksDB); key-count *can* grow online but only
via hash-table migration (Redis incremental, or FASTER epoch-phased); physical placement is solved by a
virtual→physical routing map with resumable migration + atomic cutover (Couchbase). The open question this
raises for Velo: **is the global safepoint reshard the right concurrency model, or should the key-count wall
use Redis-style per-op incremental rehash** (which suits the single-owner thread model and avoids a brownout)?

## Alternative Considered: Multi-Process

Run **N single-slot Velo processes on the host behind a thin local proxy**, and scale with the existing
cluster migrate. This is "a cluster on a local node" literally, and it **reuses 100% of the existing
machinery** — it sidesteps walls #1–#3 entirely (each process has its own fixed `slotNumber`, its own threads,
its own binlog/handshake).

- **Pros:** far less new code; no in-process threading surgery; no `@ThreadNeedLocal` rehome; migration is the
  already-built cluster path.
- **Cons:** extra processes and per-process fixed overhead; a proxy hop on every request; cross-process
  memory is not shared.

The in-process safepoint design should be **justified against this baseline**, not assumed superior. If the
proxy hop and process overhead are acceptable, multi-process is a much cheaper route to the same goal.

## Approach: Shadow 2× Rebuild via Replication (likely v1)

Stand up a **shadow slot set at the new (e.g. 2×) `slotNumber` and `slotWorkers`**, freshly initialized at the
target topology. The shadow back-fills existing data and **catches up via binlog like a slave**; when caught
up, **atomically switch** traffic to the shadow and **drop the old** slot set. Optional / metric-driven: only
trigger when workload metrics justify it. This is the replication-based generalization of the Multi-Process
alternative, and it matches industry online-resharding (Vitess **VReplication**, `gh-ost`, Couchbase **swap
rebalance**): back-fill a new-topology target, tail the log to converge, atomic cutover.

This is the **easiest path to a *correct* first version** — it leans on the most hardened subsystem (binlog +
slave catch-up + failover via `LeaderSelector`), and rollback is trivial (drop the shadow; the old set is
untouched). But it is "easiest to make correct," not "cheapest on resources."

### Critical condition: the shadow must be separately *initialized*

The win exists **only if the shadow is a separate process (or at least a separately-initialized slot set)**,
built fresh at the new topology the way slots are built at startup today. Then it never *grows* anything in
place and sidesteps the walls:

- Wall #1 (slot array sized once) — shadow's `OneSlot[2N]` is allocated fresh at its own init, not grown.
- Wall #2 (`slot % slotWorkers` fixed, no runtime rehome) — shadow has `2×slotWorkers` from its own startup;
  no `@ThreadNeedLocal` rebuild, no mutable binding. "2× slot worker" is just the shadow's config.

If instead the 2N shadow slots are added **into the running process**, you hit walls #1/#2 head-on (grow the
live `OneSlot[]`, add worker threads at runtime) — i.e. same-process shadow buys nothing. **Separate-init is
what makes this easy.**

### Why the doubling-hostile hash does not hurt here

For in-place split, the slot hash being `slotNumber`-dependent (`BaseCommand.java:637-649`) was a blocker.
Here it is irrelevant: the shadow **re-hashes every key under the new `slotNumber` anyway** (full rebuild), so
it never relies on an old→new slot relationship. It just means the copy is **key-by-key**, not a slot-file
move — which a full rebuild does regardless.

### Costs (where "easiest" stops being free)

1. **Transient 2×–3× resources on the same host** — old set (N slots, full data) and shadow (2N slots, a
   second full copy) run simultaneously. Scale-up is usually triggered *because* capacity is short, so this
   needs the most headroom at the worst moment — **especially disk** (two full data copies on one disk). The
   dominant weakness; if the host is near its limit, the shadow will not fit.
2. **A new "re-shard replication" apply mode** — vanilla slave is slot-to-slot at *equal* `slotNumber`; here
   each binlog entry must be **re-hashed under the new `slotNumber` and fanned out** to a possibly-different
   shadow slot/worker. And the handshake must **relax constraint #4** (`slaveCanMatch`, `ConfForSlot.java:165`,
   currently rejects a `slotNumber` mismatch). This modifies the safety-critical replication path.
3. **Copies ~100% of the data, not ~50%** — because the hash is doubling-hostile almost every key moves; an
   aligned/incremental reshard would move only the fraction that changes owners. Shadow-2× is the simplest
   point on the spectrum but the most data movement and transient space.
4. **Catch-up convergence + downstream slaves** — apply rate must exceed write rate to reach a short switch
   window; existing downstream slaves must re-replicate from the new topology after switch.

### Verdict

- Host **has** spare disk/RAM for a transient second copy → this is the pragmatic **v1**, preferred over the
  in-place safepoint reshard (less new code, trivial rollback, brownout only at the switch).
- Scale-up triggered by being **out** of headroom → the transient 2× is exactly what you cannot afford; an
  in-place / incremental approach wins despite being harder.

## Phased Roadmap

- **Phase 0 — Observability + elastic containers (cheap, do first).** Export per-slot fill ratios in
  `OneSlot.collect()` (keys vs `bucketsPerSlot × split`, used segments vs `maxSegmentNumber`).
  Investigate making chunk capacity elastic — e.g. the "unbounded chunk tail" idea borrowed from RocksDB's
  bottom level (see Reference section, idea #1; resolves constraint #7). Independently shippable; could cover
  most near-term value-bytes pain with no migration.
- **Phase 1 — Routing indirection on the single-host path (keystone).** Generalize `MultiShard`/`SlotRange`
  for `clusterEnabled = false`; route `virtualSlot → physicalSlot` through a table in `BaseCommand.slot()`
  instead of `keyHash / x`. Backward-compatible default: one physical slot owns all ranges. Prove parity with
  existing slot tests first (TDD).
- **Phase 2 — Space-oriented safepoint reshard.** Add physical slots to existing workers; safepoint reshard
  with copy-then-cutover; reconfig journal; binlog topology record for slaves. This covers space pressure
  without live slot-worker growth or cross-thread rehome.
- **Phase 3 — CPU scale-up by restart / replica promotion.** Treat higher `slotWorkers` as a startup topology
  change. Use rolling restart, shadow replacement, or replica promotion to move traffic to an instance started
  with more slot workers.
- **Phase 4 — Optional live slot rehoming + autoscale.** Move slots between workers for throughput only if the
  restart/promotion path is not acceptable in practice; a controller under `dyn/ctrl/` can use Phase-0 metrics
  within a max-pause budget.

### Key-Bucket Capacity Reality Check

With the current upper-bound constants, key-bucket capacity is large enough that normal key-count growth is
probably **not** the first single-host scale-up risk:

```text
MAX_BUCKETS_PER_SLOT = 16384 * 16 = 262144 bucket indexes
KeyBucket.INIT_CAPACITY = 48 key cells per bucket page
KeyLoader.MAX_SPLIT_NUMBER = 9

one split fd file capacity per slot = 262144 * 48 = 12,582,912 key cells
max key-cell capacity per slot = 262144 * 48 * 9 = 113,246,208 key cells
```

So the key-bucket wall should be framed as a **skew / pathological-distribution risk**, not the default v1
scale-up driver. A single hot bucket can still exhaust its 9 split pages while the slot average looks safe, and
large split numbers can hurt batch persist/rebuild paths before a hard full. Smaller `bucketsPerSlot` configs
also reduce this headroom. But for max-bucket deployments, value bytes and slot-worker throughput are more
likely to drive scale-up pressure than raw key-count capacity.

Practical priority:

| Pressure | Likely scale-up urgency |
|----------|-------------------------|
| Value bytes / chunk segments | Highest; direct capacity wall and already visible through `chunk_segment_fill_rate`. |
| Throughput / CPU per slot worker | Workload dependent; important for hot slots. |
| Key buckets | Mostly skew diagnostics and warning; not usually the first capacity wall at max settings. |

### Phase-0 Cap-Warning Metrics

`chunk_segment_fill_rate` already covers the most direct value-bytes capacity wall. The missing early-warning
surface is key-bucket skew: total key count alone is not enough, because one skewed bucket can exhaust its
split headroom while the slot average fill still looks safe. Fortunately
[`StatKeyCountInBuckets`](/home/kerry/ws/velo/src/main/java/io/velo/persist/StatKeyCountInBuckets.java)
already keeps per-bucket key counts in memory, so skew metrics can be computed cheaply without scanning
key-bucket pages.

Add per-slot key-bucket capacity metrics:

| Metric | Meaning |
|--------|---------|
| `key_bucket_capacity_total` | Sum of effective cell capacity across all buckets and their current split numbers. |
| `key_bucket_fill_rate` | `persist_key_count / key_bucket_capacity_total`. |
| `key_bucket_key_count_avg` | Average keys per bucket index. |
| `key_bucket_key_count_max` | Hottest bucket key count. |
| `key_bucket_key_count_p95` / `p99` | Tail of the per-bucket key-count distribution. |
| `key_bucket_skew_ratio_max_to_avg` | Hottest bucket divided by average bucket count. |
| `key_bucket_non_empty_count` | Number of bucket indexes with at least one key. |
| `key_bucket_high_fill_count` | Buckets at or above a warning threshold, e.g. 70% of local capacity. |
| `key_bucket_full_risk_count` | Buckets at or above a danger threshold, e.g. 90% of local capacity. |
| `key_bucket_split_number_max` | Largest split number currently used by any bucket. |
| `key_bucket_split_number_avg` / `p95` | Distribution of split headroom consumption. |
| `key_bucket_max_split_bucket_count` | Buckets already at the maximum split number. |

The most important alert conditions are `key_bucket_full_risk_count > 0`,
`key_bucket_max_split_bucket_count > 0`, and a rising `key_bucket_skew_ratio_max_to_avg`. They warn about skew-driven
key-bucket exhaustion before `BucketFullException` appears.

Add node-level rollups across slots so an operator does not need to inspect every slot manually:

| Metric | Meaning |
|--------|---------|
| `slot_key_count_max` / `p95` | Slot-level key-count skew. |
| `slot_key_count_skew_ratio_max_to_avg` | Hottest slot divided by average slot key count. |
| `slot_chunk_segment_fill_rate_max` / `p95` | Worst value-capacity pressure across slots. |
| `slot_key_bucket_fill_rate_max` / `p95` | Worst key-bucket capacity pressure across slots. |
| `slot_key_bucket_full_risk_count_max` | Worst full-risk bucket count for any slot. |

Also expose big-string pressure separately from normal chunk pressure:

| Metric | Meaning |
|--------|---------|
| `big_string_files_disk_usage` | Existing disk-use metric for external big-string files. |
| `big_string_uuid_by_key_size` | Existing in-memory big-string tracking size. |
| `big_string_files_count` | Number of external big-string files, useful for inode/file-count pressure. |
| `big_string_files_overwrite_pending_count` | Backlog of obsolete big-string files waiting for deletion. |

Do **not** treat WAL occupancy as a scale-up trigger in this design. WAL files are bounded by delayed-write
buffers and are roughly 10% of chunk-file capacity, so they are useful operational metrics but not primary
capacity-wall metrics for single-host scale-up.

## Open Decisions

- **Top-level approach** — Shadow 2× rebuild via replication (likely v1, needs transient 2× resources) vs
  in-place safepoint reshard (no 2× resources, harder) vs Multi-Process. Settle this first.
- **Shadow: separate process vs same process** — separate-init is what dodges walls #1/#2; same-process shadow
  buys nothing.
- **In-process vs multi-process** — settle against the alternative above before committing to Phase 1.
- **Virtual-slot count and definition** — reuse Redis's 16384, or define from `keyHash` high bits to align
  ranges with bucket ranges (cheaper migration). Default to the keyHash-aligned scheme.
- **Concurrency model for online space growth** — global safepoint reshard (simplest, brownout) vs Redis-style
  per-op incremental rehash for the key-count wall (no brownout, fits the single-owner thread model) vs
  FASTER-style epoch-phased resize (no brownout, most machinery). See the hash-index Reference section.
- **CPU scale-up policy** — v1 should use rolling restart / replica promotion with a higher `slotWorkers`
  value; live worker addition and slot rehome are deferred unless restart/promotion proves unacceptable.
- **Global vs localized collapse** for v1 space reshard — global is simpler, localized is more available.
- **Homogeneous vs heterogeneous per-slot `bucketsPerSlot`** (constraint #1 / hard problem #6).

## Related Documents

- [Persistence Layer](/home/kerry/ws/velo/doc/design/02_persist_layer_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
- [Cluster Management](/home/kerry/ws/velo/doc/design/16_cluster_management_design.md)
- [Multithreading](/home/kerry/ws/velo/doc/design/11_multithreading_design.md)

## Update: 2N Shadow Replication Is Implemented (v1)

The "Shadow 2× Rebuild via Replication" approach described above is now implemented for the specific case of
`slave slotNumber == master slotNumber * 2`. Implementation details are in
[Replication Design § 2N Scale-Up Slave Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md) and the
plan at `docs/plans/2026-06-25-master-n-slave-2n-replication.md`.

What v1 provides:
- Only stream slots `[0, N)` connect to the master; extra slots `[N, 2N)` receive data via fan-out.
- A central, race-free global read gate gates all `2N` slots on every stream's readiness.
- Incremental replay is awaited before the fetched-offset advances (await-before-advance).
- Promotion restores all `2N` slots to master state.

What v1 does **not** provide (known limitations):
- `FLUSHALL`/`FLUSHDB` stalls the 2N rebuild (stuck-but-safe) until a cross-stream flush barrier exists.
- A promoted 2N node has no binlog history for extra slots — no downstream replication for `[N, 2N)`.
- Only exactly `2N` is supported; general `N -> M` and `clusterEnabled=true` need separate designs.
- The in-process threading/topology growth (Phase 2 safepoint reshard) remains a future task; 2N shadow replication
  is the separate-init path that dodges walls #1/#2 by requiring a fresh `slotNumber` at startup.
