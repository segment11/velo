# Bug 89 — UGroup / WGroup Commands Review (Round 1)

**Module:** `io.velo.command.UGroup` (`unsubscribe`) and `io.velo.command.WGroup` (`wait`), plus the
`SocketInspector` / `OneSlot` helpers they call
**Round:** 1
**Author:** AI agent 1
**Reviewer:** AI agent 2
**Status:** round 1 closed — 8 findings `CONFIRMED (reproduced)` and fixed in 9 commits, 89.9 `REFUTED` (no change needed); **stage 5 re-review ACCEPTED** (agent 2: 8 REDs re-derived, focused tests green at HEAD, JaCoCo confirmed, wire byte-identical to Redis 7.2.11) with 4 non-blocking follow-ups and the `SGroup.subscribe()` sibling half still open
**Review branch:** `review/ugroup_wgroup`
**Reviewed commit:** `6cf5c303f7e210e031f6c957a752c170d7c72ec2`
**Fixed HEAD:** `a59c1bd6` (re-review baseline)

## Scope

Files read (method + line ranges):

- `src/main/java/io/velo/command/UGroup.java` — static registry (16-25), `parseSlots()` (44-48), `handle()` (55-63),
  `unsubscribe(boolean)` (65-89)
- `src/main/java/io/velo/command/WGroup.java` — static registry (17-26), `parseSlots()` (45-49), `handle()` (56-64),
  `cmdWait()` (66-132)
- `src/main/java/io/velo/SocketInspector.java` — `subscribe()` (526-564), `unsubscribe()` (574-579),
  `subscribeChannels()` (640-656), `onDisconnect()` (950-984)
- `src/main/java/io/velo/command/PGroup.java` — `handle()` dispatch of `psubscribe`/`punsubscribe` (278-301, the
  latter delegates to `UGroup.unsubscribe(true)`)
- `src/main/java/io/velo/command/SGroup.java` — `subscribe(boolean)` (2417-2452), for the sibling confirmation
  triple shape/count
- `src/main/java/io/velo/BaseCommand.java` — the `SocketInspector.updateLastSetSeq()` call site (1263)
- `src/main/java/io/velo/persist/OneSlot.java` — `isAsSlave()` (357-372), `getSlaveReplPairListSelfAsMaster()` (379+),
  `delayRun()` (757-764)
- `src/main/java/io/velo/VeloUserDataInSocket.java` — `lastSetSeq` / `lastSetSlot` (63-91)
- `src/main/java/io/velo/reply/ErrorReply.java` — `FORMAT` (14), `UNKNOWN_COMMAND` (28), `INVALID_INTEGER` (76)
- `src/main/java/io/velo/command/CommandRegistry.java` — whole (31-110): metadata only, arity is **not** enforced at
  dispatch; the per-group `data.length` checks are the real arity enforcement
- `src/main/java/io/velo/RequestHandler.java` — command group wiring (156-158)
- `src/test/groovy/io/velo/command/UGroupTest.groovy` (1-69), `src/test/groovy/io/velo/command/WGroupTest.groovy` (1-167)
- Prior rounds (not re-reported items): `bug_84_pgroup_commands_review_round_1.md` (Scope lists `UGroup.unsubscribe()`;
  Residual Risk #2 empty-map entry, #3 glob matcher), `bug_86_sgroup_commands_review_round_1.md` (non-findings:
  subscribe reply shape note), `bug_85` (85.12/85.15 unknown-command behavior), `STATE.md`

Reference docs (ground truth — local Redis 7.2.14 sources `~/ws/redis`, `REDIS_VERSION 7.2.14`):

- `src/pubsub.c` — `addReplyPubsubUnsubscribed()` (155-169), `pubsubUnsubscribeChannel()` (273-308),
  `pubsubUnsubscribeAllChannelsInternal()` (399-421), `unsubscribeCommand()` (553-563)
- `src/replication.c` — `replicationCountAcksByOffset()` (3503-3516), `waitCommand()` (3537-3567),
  `processClientsWaitingReplicas()` (3628-3670), `unblockClientWaitingReplicas()` (3615-3621)
- `src/blocked.c` — `blockForReplication()` (601-606), `blockedBeforeSleep()` (748-757, calls
  `processClientsWaitingReplicas()` every event loop iteration)
- `src/timeout.c` — `getTimeoutFromObjectOrReply()` (161-195: 0 is valid, negative → `timeout is negative`)
- `src/server.c` — `c->woff` assignment after a propagated write (3690-3696)
- `src/commands.def` — `wait` (10689: arity 3, `ACL_CATEGORY_CONNECTION`), `unsubscribe` (10757: arity -1,
  `CMD_PUBSUB|CMD_NOSCRIPT|CMD_LOADING|CMD_STALE|CMD_SENTINEL`)
- https://redis.io/docs/latest/commands/unsubscribe/ , https://redis.io/docs/latest/commands/wait/
- Design docs: `doc/design/04_command_processing_design.md`, `doc/design/09_replication_design.md` (no WAIT- or
  UNSUBSCRIBE-specific contract found — the per-slot replication model is documented there),
  `doc/redis_command_support.md` (UNLINK / WAIT / WAITAOF marked `[x]`)

## Finding 1 (Bug 89.1): WAIT always sleeps the full timeout — never replies early, even when the replicas already acked

**Severity:** High
**Class:** Correctness | Redis protocol compatibility (latency)
**Status:** `CONFIRMED (reproduced)` — **fixed** (`11fc5087`), re-review ACCEPTED (see Re-review)

### Cited files / lines
```java
// WGroup.java:106-131 — the only check happens once, after the full timeout
SettablePromise<Reply> finalPromise = new SettablePromise<>();
var asyncReply = new AsyncReply(finalPromise);

oneSlot.delayRun(timeoutMillis, () -> {
    // check again
    var slaveReplPairList2 = oneSlot.getSlaveReplPairListSelfAsMaster();
    ...
    finalPromise.set(new IntegerReply(catchUpSlaveCount));
});
```
```c
// redis/src/replication.c:3553-3558 — first try without blocking at all
ackreplicas = replicationCountAcksByOffset(c->woff);
if (ackreplicas >= numreplicas || c->flags & CLIENT_DENY_BLOCKING) {
    addReplyLongLong(c,ackreplicas);
    return;
}
// redis/src/blocked.c:748-757 — and while blocked, re-checked every event loop iteration
void blockedBeforeSleep(void) {
    ...
    if (listLength(server.clients_waiting_acks))
        processClientsWaitingReplicas();
```

### Root cause
`cmdWait()` computes nothing before scheduling `oneSlot.delayRun(timeoutMillis, ...)` (OneSlot.java:757-764 just
delegates to `slotWorkerEventloop.delay()`), and the delayed runnable is the **only** place the slave catch-up
seqs are compared (`slaveReplPair.getSlaveCatchUpLastSeq() >= lastSetSeq`, WGroup.java:120). There is no immediate
first-try check and no polling while waiting — the catch-up state is sampled exactly once, at the deadline.

### Trigger / impact
Any `WAIT` on a healthy replication pair. `WAIT 1 60000` where the single slave is already caught up: Redis answers
in well under a millisecond (first-try path, replication.c:3553-3558); Velo holds the client for the full 60 s.
Same for the not-yet-caught-up case: Redis unblocks the client in the first `beforeSleep` iteration after the acks
arrive (blocked.c:753-755 → replication.c:3628+); Velo still answers only at the deadline, reporting the count that
was true at the deadline. Every satisfied WAIT pays the full configured latency — a 3-orders-of-magnitude latency
divergence for the common case, and the returned count can be stale-by-timeout rather than "as soon as satisfied".

### Suggested fix direction
Before scheduling, check the catch-up count immediately and settle `finalPromise` synchronously when
`catchUpSlaveCount >= numReplicas` (mirroring the Redis first-try). Otherwise poll with a short interval (e.g. 10-50 ms
`delayRun` chain) until satisfied or the deadline expires. Keep the reply an `AsyncReply` only on the truly
unsatisfied path.

## Finding 2 (Bug 89.2): `UNSUBSCRIBE` with no arguments answers `syntax error`; Redis unsubscribes from all channels

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** (`97440cc4`), re-review ACCEPTED (error text refined by the reviewer, see Review Feedback)

### Cited files / lines
```java
// UGroup.java:66-69
Reply unsubscribe(boolean isPattern) {
    if (data.length < 2) {
        return ErrorReply.FORMAT;   // "syntax error"
    }
```
```c
// redis/src/pubsub.c:553-562
void unsubscribeCommand(client *c) {
    if (c->argc == 1) {
        pubsubUnsubscribeAllChannels(c,1);
    } else { ... }
// redis/src/pubsub.c:413-417 — even subscribed to nothing, still reply
    /* We were subscribed to nothing? Still reply to the client. */
    if (notify && count == 0) {
        addReplyPubsubUnsubscribed(c,NULL,type);   // ["unsubscribe", nil, 0]
    }
```

### Root cause
The guard treats 0 channels as invalid input. Redis arity for UNSUBSCRIBE is -1 (commands.def:10757) and no-arg
means "unsubscribe all"; a client subscribed to nothing gets a single `["unsubscribe", nil, 0]` triple
(pubsub.c:415-417 + 163-166 `addReplyNull`). `PUNSUBSCRIBE` (arity -1 too, commands.def:10752) has the identical
behavior and reaches the same guard through `PGroup.handle()` → `UGroup.unsubscribe(true)` (PGroup.java:298-301).

### Trigger / impact
`UNSUBSCRIBE` (no args) — the standard "leave pubsub mode" idiom used by clients after `SUBSCRIBE`; jedis'
`JedisPubSub.unsubscribe()` issues exactly this. The client gets `ERR syntax error` instead of the confirmation
triples, and its subscriptions are left in place. `PUNSUBSCRIBE` (no args) is equally affected.

### Suggested fix direction
When `data.length == 1`: iterate this socket's subscriptions (see 89.3 — a per-client view is needed), emit one
triple per unsubscribed channel, or `["unsubscribe", nil, 0]` (`["punsubscribe", nil, 0]`) when there are none.
A `NilReply.INSTANCE` for the channel slot works for the RESP2 encoding.

> **Reviewer verdict (agent 2): `CONFIRMED (reproduced)`, error text refined.** The defect is real, but the
> wire text is **not** `ERR syntax error` — the net layer rewrites the `ErrorReply.FORMAT` sentinel into
> `ERR wrong number of arguments for 'unsubscribe' command` (`MultiWorkerServer.java:611-615`). Redis answers
> `*3 $11 unsubscribe $-1 :0`. Fix direction unchanged; the fix must not reuse `ErrorReply.FORMAT` for the
> reply, because that sentinel is reserved for arity errors.

## Finding 3 (Bug 89.3): the confirmation count is "sockets remaining on the channel (server-wide)", Redis returns this client's remaining subscriptions

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** (`490a627b` + `4e8ec8b2`), re-review ACCEPTED

### Cited files / lines
```java
// UGroup.java:81-86 — the integer is whatever SocketInspector.unsubscribe() returns
for (var channel : channels) {
    ...
    var size = socketInInspector.unsubscribe(channel, isPattern, socket);
    replies[j++] = new IntegerReply(size);
}
// SocketInspector.java:574-579 — returns the number of OTHER sockets (or the same one, elsewhere) still on the channel
public int unsubscribe(@NotNull String channel, boolean isPattern, ITcpSocket socket) {
    var one = new ChannelAndIsPattern(channel, isPattern);
    var sockets = subscribeByChannel.computeIfAbsent(one, k -> new ConcurrentHashMap<>());
    sockets.remove(socket);
    return sockets.size();
}
```
```c
// redis/src/pubsub.c:167 — the count is per-client, per-type, AFTER removal
    addReplyLongLong(c,type.subscriptionCount(c));
```

### Root cause
`SocketInspector` tracks channel → sockets, with no per-socket subscription view, so `unsubscribe()` can only return
the channel's remaining socket count. Redis's third field of the confirmation triple is
`clientSubscriptionsCount(c, type)` — how many channels (or patterns, for punsubscribe) **this client** is still
subscribed to after the operation.

### Trigger / impact
Client A alone subscribes `a` and `b`, then `UNSUBSCRIBE a`: Redis → `["unsubscribe","a",1]` (A still holds `b`);
Velo → `["unsubscribe","a",0]` (no sockets left on `a`). Two clients on channel `a`, one runs `UNSUBSCRIBE a`:
Velo answers `1` (the other client's socket) although that client may hold no other subscription at all. The count
drifts in both directions. Clients and tests that assert the countdown (the standard "subscribe N channels, watch
the count decrease to 0" idiom) misbehave. Note the same family exists on the SUBSCRIBE side
(`SGroup.subscribe()`, SGroup.java:2447-2448 returns `subscribe()`'s `sockets.size()`) — that class was reviewed by
bug_86 without this being reported; filed here because the unsubscribe half lives in UGroup.

### Suggested fix direction
Maintain a per-socket subscription count (or set) — e.g. a `ConcurrentHashMap<ITcpSocket, Set<ChannelAndIsPattern>>`
or counting the socket's entries by scanning (acceptable at this scale) — and have the reply use this client's
remaining count of the same type (channels vs patterns separately, as Redis does).

## Finding 4 (Bug 89.4): multi-channel `UNSUBSCRIBE` returns one 3N-element array instead of N separate 3-element replies

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** (`a59c1bd6`, the UGroup half), re-review ACCEPTED

### Cited files / lines
```java
// UGroup.java:79-88 — all triples flattened into a single MultiBulkReply
var replies = new Reply[channels.size() * 3];
int j = 0;
for (var channel : channels) {
    replies[j++] = new BulkReply(isPattern ? "punsubscribe" : "unsubscribe");
    replies[j++] = new BulkReply(channel);
    var size = socketInInspector.unsubscribe(channel, isPattern, socket);
    replies[j++] = new IntegerReply(size);
}
return new MultiBulkReply(replies);
```
```c
// redis/src/pubsub.c:273-308 — pubsubUnsubscribeChannel() calls addReplyPubsubUnsubscribed() once per channel,
// each adding its own *3 array (pubsub.c:158-161)
```

### Root cause
The reply model supports exactly one RESP value per command, so N confirmations are flattened into one array of
3N elements (`*6` for two channels). Redis streams N independent `*3` arrays on the socket (one per
`pubsubUnsubscribeChannel()` call). bug_86's non-findings line "reply shape (3 fields per channel) correct"
(SGroup subscribe) only holds for the single-channel case; the multi-channel frame split was never analyzed.

### Trigger / impact
`UNSUBSCRIBE a b` (or `SUBSCRIBE a b` on the SGroup side): a client reading one confirmation frame per channel —
the documented protocol (redis.io/docs/latest/commands/unsubscribe: "one message for each channel") — receives a
single 6-element array and desynchronizes or errors. Single-channel callers are unaffected, which is why this
survived the bug_84/bug_86 differentials.

### Suggested fix direction
The reply abstraction needs a multi-frame reply (list of `Reply`s written back-to-back), or the RequestHandler
write path for pubsub commands must accept `List<Reply>`. Same fix applies to `SGroup.subscribe()`; if the fix is
done centrally, keep it one commit per class or note the shared refactor explicitly.

## Finding 5 (Bug 89.5): `WAIT 0 <t>` and `WAIT <n> 0` rejected with "value is not an integer or out of range"

**Severity:** Medium
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** (`eee46598`), re-review ACCEPTED

### Cited files / lines
```java
// WGroup.java:80-82
if (numReplicas <= 0 || timeoutMillis <= 0) {
    return ErrorReply.INVALID_INTEGER;
}
```
```c
// redis/src/replication.c:3548-3551 — plain long parse, no positivity constraint on numreplicas
    if (getLongFromObjectOrReply(c,c->argv[1],&numreplicas,NULL) != C_OK) return;
    if (getTimeoutFromObjectOrReply(c,c->argv[2],&timeout,UNIT_MILLISECONDS) != C_OK) return;
// redis/src/timeout.c:161-195 — 0 is a legal timeout ("no timeout"), only tval < 0 errors ("timeout is negative")
// redis/src/replication.c:3553-3558 — numreplicas==0 (and any numreplicas <= ackreplicas) answers immediately
```

### Root cause
The guard conflates "non-integer" with "non-positive". In Redis 7.2 `numreplicas` 0 (or negative) is accepted and
answers immediately via the first-try path (`ackreplicas >= numreplicas` is trivially true), and `timeout` 0 means
"block until satisfied, never time out" (blocked.c:601-606 stores it; the timeout table only arms non-zero
deadlines). Only a negative timeout errors, with text `timeout is negative`.

### Trigger / impact
`WAIT 0 0` — the cheapest liveness probe of the replication link — errors in Velo but returns the current ack
count in Redis. `WAIT 1 0` likewise. The existing test `WGroupTest.groovy:136-144` asserts the divergent behavior
(`'wait 0 0' | ErrorReply.INVALID_INTEGER`), so the regression test itself encodes the wrong oracle.

### Suggested fix direction
Parse as long; reject only `numReplicas < 0`-style invalids per the chosen semantics (Redis accepts negatives —
simplest is to accept `<= 0` and follow the first-try path) and reject only `timeout < 0` (with
`timeout is negative` or the shared error text). `WAIT 0 t` then takes the immediate-reply path from 89.1;
`WAIT n 0` polls until satisfied. Update the two test rows accordingly.

## Finding 6 (Bug 89.6): WAIT from a client that never wrote answers 0; Redis counts every online replica

**Severity:** Medium
**Class:** Correctness | Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** (`352c5d50`), re-review ACCEPTED (with Follow-up 1)

### Cited files / lines
```java
// WGroup.java:88-92
var veloUserData = SocketInspector.createUserDataIfNotSet(socket);
var lastSetSeq = veloUserData.getLastSetSeq();
if (lastSetSeq == 0L) {
    return IntegerReply.REPLY_0;
}
```
```c
// redis/src/server.c:3690-3696 — c->woff is only advanced by this client's propagated writes; it starts at 0
    if (old_master_repl_offset != server.master_repl_offset)
        c->woff = server.master_repl_offset;
// redis/src/replication.c:3513 — offset 0 is satisfied by every online slave
        if (slave->repl_ack_off >= offset) count++;
```

### Root cause
Velo maps "no writes issued by this connection" to "nothing to wait for → 0". Redis semantics: a fresh client's
`woff` is 0, `replicationCountAcksByOffset(0)` counts every `SLAVE_STATE_ONLINE` replica (repl_ack_off is
unsigned-large), so `WAIT N T` on a fresh connection returns the number of connected caught-up replicas
(effectively a replica liveness count), not 0.

### Trigger / impact
A client that connects and immediately runs `WAIT 2 100` (monitoring script, health check) gets 0 from Velo even
with 2 fully caught-up slaves; Redis returns 2. The answer under-reports replication health exactly in the
health-check use case where WAIT-with-no-prior-write is used.

### Suggested fix direction
When `lastSetSeq == 0`, treat the wait offset as trivially satisfied: run the same count logic with
`lastSetSeq = 0` (every slave with `getSlaveCatchUpLastSeq() >= 0` counts) instead of short-circuiting to 0.
If the product prefers Velo's stricter reading, it should be recorded as an explicit won't-fix divergence.

## Finding 7 (Bug 89.7): WAIT on a replica answers integer 0; Redis answers an explicit error

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** (`87550181`), re-review ACCEPTED (with Follow-up 2)

### Cited files / lines
```java
// WGroup.java:94-98
var oneSlot = localPersist.oneSlot(veloUserData.getLastSetSlot());
var isAsSlave = oneSlot.isAsSlave();
if (isAsSlave) {
    return IntegerReply.REPLY_0;
}
```
```c
// redis/src/replication.c:3542-3545
    if (server.masterhost) {
        addReplyError(c,"WAIT cannot be used with replica instances. Please also note that since Redis 4.0 if a replica is configured to be writable (which is not the default) writes to replicas are just local and are not propagated.");
        return;
    }
```

### Root cause
Velo silently maps the replica case to 0; Redis refuses the command with a descriptive error (a writable replica's
local writes are not propagated, so WAIT there is meaningless).

### Trigger / impact
`WAIT` issued against a slot acting as slave: a client cannot distinguish "you are on a replica" from "0 replicas
acked" — it will retry or misreport replication health instead of failing fast. Error text divergence only.

### Suggested fix direction
Return an `ErrorReply` with the Redis text when the slot acts as slave (mind the per-slot model: only the
`lastSetSlot` is consulted — see Notes for the architectural caveat).

## Finding 8 (Bug 89.8): WAIT timeout is capped at 60 s with a non-Redis error

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `CONFIRMED (reproduced)` — **fixed** (`65031dea`), re-review ACCEPTED

### Cited files / lines
```java
// WGroup.java:83-86
final int maxTimeoutMillis = 1000 * 60;
if (timeoutMillis > maxTimeoutMillis) {
    return new ErrorReply("timeout should <= " + maxTimeoutMillis);
}
```
```c
// redis/src/timeout.c:180-195 — the only range checks are negative and overflow; multi-minute timeouts are legal
    if  (tval > LLONG_MAX - now) { addReplyError(c, "timeout is out of range"); ... }
```

### Root cause
A protective bound that Redis does not have, with an error text that matches no Redis message (and reads like a
typo: "timeout should <= 60000").

### Trigger / impact
`WAIT 1 120000` (a legitimate 2-minute durability wait) errors in Velo, blocks in Redis. Low frequency, but a hard
compat break for clients that derive their timeout from config.

### Suggested fix direction
Either accept any non-negative timeout (the poll loop from 89.1 makes the cap unnecessary — the connection is not
blocked, only the reply is pending) or clamp silently; if a cap must stay, document it as a product decision. Any
kept error should use a Redis-style text.

## Finding 9 (Bug 89.9): ~~wrong-arity WAIT answers "syntax error" instead of "wrong number of arguments for 'wait' command~~" — RETRACTED by agent 1 (stage 3)

**Severity:** Low
**Class:** Redis protocol compatibility
**Status:** `RETRACTED` — no change needed (the net layer already rewrites the arity error to the redis text); the reviewer's `REFUTED` verdict was accepted by agent 1 in stage 3 and re-confirmed at stage 5 (live wire: `-ERR wrong number of arguments for 'wait' command`)

### Cited files / lines
```java
// WGroup.java:67-69
private Reply cmdWait() {
    if (data.length != 3) {
        return ErrorReply.FORMAT;
    }
```
```
// Redis: any arity violation (commands.def arity 3 is enforced in processCommand)
ERR wrong number of arguments for 'wait' command
```

### Root cause
The registry arity (3) is not enforced at dispatch (CommandRegistry is metadata-only — see Scope), so the
in-command fallback fires, and it uses the generic `syntax error` text. Note `UNSUBSCRIBE`'s no-arg case is **not**
this bug — 89.2 shows no-arg is legal there; the same one-line guard serves two different semantics.

### Trigger / impact
`WAIT` / `WAIT 1` / `WAIT 1 2 3` → `ERR syntax error` instead of Redis' wrong-number-of-arguments error. Clients
that match on the error text (orchestrators, redis-cli hints) misreport. Related known residual: the velo-wide
`ErrorReply.WRONG_NUMBER` stray `*` prefix (bug_83 follow-up) — fixing this should reuse/repair that constant
rather than add another text.

### Suggested fix direction
Use the Redis arity error text for `data.length != 3` (via the repaired `WRONG_NUMBER`-style helper or a local
`new ErrorReply("wrong number of arguments for 'wait' command")`).

> **Reviewer verdict (agent 2): `REFUTED` — no change needed.** The analysis above stops at the command
> object and misses the net layer: `ErrorReply.FORMAT` is an internal sentinel that
> `MultiWorkerServer` rewrites into `ErrorReply.WRONG_NUMBER(cmd)` before writing it to the socket
> (`MultiWorkerServer.java:611-615`, `:674-678`, `:702-704`). The live wire capture from a real velo server
> shows `-ERR wrong number of arguments for 'wait' command` for `wait`, `wait 1` and `wait 1 2 3` — byte
> identical to redis. The parenthetical claim about a stray `*` prefix in `ErrorReply.WRONG_NUMBER` is also
> stale (the constant is correct, `RESPReplyTest.groovy:58-59`). The original text of this finding is kept
> for traceability; see the Review Feedback section for the evidence and the caveat it implies for the
> 89.2/89.5/89.7/89.8 fixes (never use `ErrorReply.FORMAT` for a non-arity error — the net layer would
> rewrite it).

## Notes / non-findings

- **`ErrorReply.FORMAT` is an internal sentinel, not a wire text** (added by agent 2 during verification):
  `MultiWorkerServer.java:611-615`, `:674-678` and `:702-704` rewrite it into
  `ErrorReply.WRONG_NUMBER(cmd)` before the reply is written, so every command that returns it answers on the
  wire with `ERR wrong number of arguments for '<cmd>' command`. The convention is documented in
  `src/test/groovy/io/velo/command/TGroupTest.groovy:71`. This is what refutes 89.9 and it also corrects the
  error text reported for 89.2.

- **UNSUBSCRIBE of a channel the client is not subscribed to still replies a triple** — matches Redis
  (pubsub.c:303-306 notifies regardless of whether the delete hit); only the count field is wrong (89.3). The
  `computeIfAbsent` empty-map leak it triggers (channel appears in `PUBSUB CHANNELS` with no subscribers) is
  already tracked as bug_84 Residual Risk #2 — not re-reported.
- **Glob matcher is Java's, not Redis'** (`ChannelAndIsPattern`) — bug_84 Residual Risk #3, not re-reported.
- **UNLINK and WAITAOF are not implemented** (no reference in `src/main/java` outside ACL category lists) → both
  fall through to `UNKNOWN_COMMAND` (UGroup.java:61-62, WGroup.java:62-63), the behavior accepted by 85.12/85.15;
  `doc/redis_command_support.md` tracks them as `[x]`. Feature gaps, not bugs of this round.
- **WAIT counts only the replicas of the client's `lastSetSlot`** (WGroup.java:94, 101) — Redis replication is
  global, Velo's is per-slot (doc/design/09_replication_design.md). Architectural divergence, product decision,
  not filed.
- **WAIT inside MULTI/EXEC** was not analyzed (async replies inside transactions are a cross-module concern —
  CGroup/XGroup territory); flagged for a possible follow-up, not filed.
- `Integer.parseInt` accepts a leading `+` (`+5`) where Redis' `string2ll` does not — negligible, not filed
  (32-bit vs 64-bit argument parsing is already a recorded bug_82 residual family).
- Registry metadata drift (cosmetic, feeds `COMMAND`/`COMMAND DOCS` output only): `wait` lists `@fast`/`@slow`
  ACL categories and `unsubscribe` omits the `sentinel` flag vs commands.def:10689/10757. Worth one line whenever
  89.x touches these entries, not a separate bug id (same policy as bug_86's metadata drift note).
- `UGroupTest.groovy` has an unused `NilReply` import (line 9) — style only.
- The immediate-reply paths (`lastSetSeq == 0`, as-slave, no slaves, WGroup.java:88-104) all return before creating
  the promise — no leak; the delayed runnable always settles `finalPromise` on the timeout path.

## Reviewer checklist (for AI agent 2)

- **89.1**: temp Spock spec or live: as master with one caught-up slave (`replPairAsMaster.slaveCatchUpLastSeq = 1L`,
  reuse `WGroupTest` mocks around line 114-122), run `execute('wait 1 100')` and measure when
  `settablePromise` settles (it must be ~immediately in Redis semantics; in current code it settles only after
  ~100 ms). Live Redis 7.2 differential: `SET a b; WAIT 1 5000` answers in μs on a caught-up pair.
- **89.2**: `execute('unsubscribe')` (data length 1) → currently `ErrorReply.FORMAT`; live Redis 7.2: bare
  `UNSUBSCRIBE` replies `*3\r\n$11\r\nunsubscribe\r\n$-1\r\n:0\r\n` on a clean connection. Also `PUNSUBSCRIBE`
  via PGroup.java:298-301.
- **89.3**: subscribe this socket to `a` and `b` (via `socketInspector.subscribe`), then `unsubscribe(false)` of
  `a` → current IntegerReply is 0 (channel sockets), Redis oracle is 1 (client still holds `b`). Second socket on
  `a` flips Velo's answer to 1 while the Redis oracle stays 1/0 per-client.
- **89.4**: `UNSUBSCRIBE a b` → `reply instanceof MultiBulkReply && replies.length == 6` (one frame). Redis sends
  two `*3` frames — check with `redis-cli --raw` or a socket-level probe (nc) against 7.2.
- **89.5**: `execute('wait 0 0')` → currently `ErrorReply.INVALID_INTEGER`; Redis 7.2 returns the ack count
  integer. `WAIT 1 0` on a caught-up pair returns immediately in Redis. Negative timeout: Redis says
  `timeout is negative`, Velo says `value is not an integer or out of range`.
- **89.6**: fresh socket (`SocketInspector.clearUserData(socket)` / new mock), slot as master with one caught-up
  repl pair, `execute('wait 1 100')` → currently `IntegerReply.REPLY_0` (WGroupTest.groovy:79-84 asserts this);
  Redis oracle with a caught-up slave would answer 1.
- **89.7**: `ReplPairTest.mockAsSlave()` + `oneSlot.replPairs.add(...)` (WGroupTest.groovy:93-99 pattern),
  `lastSetSeq != 0` → currently `REPLY_0`; Redis oracle is the `WAIT cannot be used with replica instances` error.
- **89.8**: `execute('wait 1 60001')` → current custom `ErrorReply`; Redis 7.2 accepts `WAIT 1 120000` (blocks).
- **89.9**: `execute('wait')` / `execute('wait 1')` → `syntax error`; Redis oracle
  `wrong number of arguments for 'wait' command`.

## Review Feedback (AI agent 2, round 1)

**Method:** live Redis 7.2.11 differential (master + 1 online replica on ports 7788/7789) **+** raw-socket
probe of a real Velo server (`build/libs/velo-1.0.0.jar`, port 41999) **+** a temporary Spock spec
(`src/test/groovy/io/velo/command/TempBug89ReviewTest.groovy`, removed after review, source pasted in
Appendix A) **+** static inspection of the cited lines. The `~/ws/redis` ground truth is Redis 7.2.x
(`REDIS_VERSION` in `src/version.h`); the installed `redis-server` used for the differential is 7.2.11, whose
`pubsub.c` / `replication.c` / `timeout.c` behaviour for the commands under review is identical to the 7.2.14
sources cited by agent 1. Full evidence log:
`doc/bug_reviews/logs/20261117_214500_ugroup_wgroup_review_agent2_bug_89.log`.

Live Redis differential transcript (master 7788 with one online replica 7789):

```
$ redis-cli -p 7788 wait 0 0
1
$ time redis-cli -p 7788 wait 1 0
real 0m0.004s        → 1
$ redis-cli -p 7788 set k v >/dev/null; time redis-cli -p 7788 wait 1 5000
real 0m0.002s        → 1            # answered in 2 ms, not after the 5 s timeout
$ redis-cli -p 7788 wait 1 -1
ERR timeout is negative
$ redis-cli -p 7788 wait -1 1000
1
$ redis-cli -p 7788 wait
ERR wrong number of arguments for 'wait' command
$ redis-cli -p 7788 wait 1 2 3
ERR wrong number of arguments for 'wait' command
$ time redis-cli -p 7788 wait 1 120000
real 0m0.001s        → 1            # no 60 s cap
$ redis-cli -p 7789 wait 1 100        # on the replica
ERR WAIT cannot be used with replica instances. Please also note that since Redis 4.0 ...
```

Live Redis pubsub transcript (raw socket, two clients A and B both subscribed to `a`):

```
A: subscribe a        → *3 $9 subscribe $1 a :1
B: subscribe a        → *3 $9 subscribe $1 a :1
A: unsubscribe a      → *3 $11 unsubscribe $1 a :0      # redis: A's own remaining count = 0
A: unsubscribe a      → *3 $11 unsubscribe $1 a :0      # redis: not subscribed, still replies
A: unsubscribe        → *3 $11 unsubscribe $-1 :0       # bare unsubscribe: unsubscribes all
A: unsubscribe a b    → *3 $11 unsubscribe $1 a :1  *3 $11 unsubscribe $1 b :0   # TWO *3 frames
```

Live Velo wire transcript (real `velo-1.0.0.jar` on port 41999, raw socket):

```
velo wait            → -ERR wrong number of arguments for 'wait' command\r\n
velo wait 1          → -ERR wrong number of arguments for 'wait' command\r\n
velo wait 1 2 3      → -ERR wrong number of arguments for 'wait' command\r\n
velo unsubscribe     → -ERR wrong number of arguments for 'unsubscribe' command\r\n
velo subscribe a b   → *6\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:1\r\n
velo unsubscribe a b → *6\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:0\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n
velo wait 0 0        → -ERR value is not an integer or out of range\r\n
velo wait 1 0        → -ERR value is not an integer or out of range\r\n
velo wait 1 60001    → -ERR timeout should <= 60000\r\n
velo wait 1 -1       → -ERR value is not an integer or out of range\r\n
velo: A unsub a while B is still subscribed → *3 $11 unsubscribe $1 a :1     # redis: :0
```

| Finding | Verdict | Evidence |
|---|---|---|
| 89.1 | `CONFIRMED (reproduced)` | Temp Spock spec: `wait 1 5000` with an already-caught-up slave settles after **4991 ms**; live redis answers the same wait in **2 ms** |
| 89.2 | `CONFIRMED (reproduced)` | Velo wire: `-ERR wrong number of arguments for 'unsubscribe' command`; redis wire: `*3 $11 unsubscribe $-1 :0` |
| 89.3 | `CONFIRMED (reproduced)` | Velo wire `:1` vs redis wire `:0` for the two-client scenario; temp spec `89.3 count = 1` |
| 89.4 | `CONFIRMED (reproduced)` | Velo wire: one `*6` frame; redis wire: two `*3` frames |
| 89.5 | `CONFIRMED (reproduced)` | Velo wire `-ERR value is not an integer or out of range` for `wait 0 0` / `wait 1 0` / `wait 1 -1`; redis returns the ack integer, and `-ERR timeout is negative` for a negative timeout |
| 89.6 | `CONFIRMED (reproduced)` | Temp spec: write-less client → `IntegerReply.REPLY_0`; live redis: fresh connection `wait 0 0` → `1` (the online replica count) |
| 89.7 | `CONFIRMED (reproduced)` | Temp spec: as-slave slot → `IntegerReply.REPLY_0`; live redis on the replica → `ERR WAIT cannot be used with replica instances ...` |
| 89.8 | `CONFIRMED (reproduced)` | Velo wire `-ERR timeout should <= 60000` for `wait 1 60001`; redis accepts `wait 1 120000` (answered in 1 ms) |
| 89.9 | `REFUTED` | Velo wire for `wait` / `wait 1` / `wait 1 2 3` → `-ERR wrong number of arguments for 'wait' command` — the net layer rewrites the `ErrorReply.FORMAT` sentinel |

### Per-finding verdicts

#### 89.1 — `CONFIRMED (reproduced)`

The temporary Spock spec reproduced the defect exactly. With one `ReplPairTest.mockAsMaster()` whose
`slaveCatchUpLastSeq = 1L` and `lastSetSeq = 1L` (i.e. the slave is **already** caught up), `wait 1 5000`
returns an `AsyncReply` whose promise settles only after the full timeout:

```
TempBug89ReviewTest > review 89.1 wait never replies early STANDARD_OUT
    89.1 settle elapsed millis = 4991
```

Live redis on a caught-up pair answers the same wait in ~2 ms (`real 0m0.002s`), i.e. through the
first-try path at `replication.c:3553-3558`. Static inspection confirms there is no first-try check in
`WGroup.cmdWait()` (`WGroup.java:106-131`): the catch-up comparison at `WGroup.java:120` lives only inside
the `oneSlot.delayRun(timeoutMillis, ...)` callback, so the state is sampled exactly once, at the deadline.
The finding's root cause and impact description are accurate. Severity **High** is justified: every
satisfied WAIT pays the full configured latency (60 s by default) and the returned count is a
deadline snapshot rather than "as soon as satisfied".

Two refinements to the fix direction:
- the "immediate first-try" must be evaluated **before** creating the `SettablePromise`/`AsyncReply`
  (as in redis), otherwise the reply is still an async frame for the common case;
- the polling loop must re-read `oneSlot.getSlaveReplPairListSelfAsMaster()` on every tick — the existing
  callback already does that at `WGroup.java:111`, keep it.

#### 89.2 — `CONFIRMED (reproduced)` (error text refined)

Velo rejects a bare `UNSUBSCRIBE`:

```
velo unsubscribe     → -ERR wrong number of arguments for 'unsubscribe' command\r\n
```

**Refinement:** agent 1's cited text is `ErrorReply.FORMAT` ("syntax error"), but the *wire* text is
"wrong number of arguments for 'unsubscribe' command", because the net layer rewrites the `ErrorReply.FORMAT`
sentinel (`MultiWorkerServer.java:613`, `:676`, `:703`; the convention is documented in
`src/test/groovy/io/velo/command/TGroupTest.groovy:71`). The finding stands — the command is still rejected
instead of unsubscribing — but the reported error text must be corrected.

Redis answers `*3 $11 unsubscribe $-1 :0` (`pubsub.c:401-419` → `addReplyPubsubUnsubscribed(c, NULL, type)`,
`pubsub.c:163-166` `addReplyNull`). `PUNSUBSCRIBE` with no arguments reaches the same guard through
`PGroup.java:298-301` and is equally affected.

#### 89.3 — `CONFIRMED (reproduced)`

Velo's count is the channel's socket count; redis' count is the *client's* remaining subscriptions:

```
# A and B both subscribe to 'a'; A unsubscribes 'a'
velo : A unsub a while B is still subscribed → *3 $11 unsubscribe $1 a :1
redis: A unsubscribe a                       → *3 $11 unsubscribe $1 a :0
```

The temp spec reproduces the same divergence in-process (`89.3 count = 1`), and `SocketInspector.unsubscribe()`
(`SocketInspector.java:574-579`) returns `sockets.size()` — the number of sockets left on the channel — while
redis returns `type.subscriptionCount(c)` = `clientSubscriptionsCount(c)` (`pubsub.c:167`), the count of
channels **this client** still holds. The `SocketInspector` keeps only a channel→sockets map
(`subscribeByChannel`), with no per-socket view, so the fix must add one; the same per-socket view is needed
by 89.2 (unsubscribe-all) and by the sibling `SGroup.subscribe()` case (bug_86 class).

#### 89.4 — `CONFIRMED (reproduced)`

Velo flattens N confirmations into one array; redis streams N independent `*3` frames:

```
velo unsubscribe a b → *6\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:0\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n
redis unsubscribe a b → *3\r\n$11\r\nunsubscribe\r\n$1\r\na\r\n:1\r\n*3\r\n$11\r\nunsubscribe\r\n$1\r\nb\r\n:0\r\n
```

The velo reply model has one `Reply` per command (`io.velo.reply` has no multi-frame type), so the fix needs
plumbing — either a new `Reply` implementation that writes several frames, or a `List<Reply>` path in the
request handler. The same defect exists on the subscribe side and is visible in the same live-velo capture:

```
velo subscribe a b → *6\r\n$9\r\nsubscribe\r\n$1\r\na\r\n:1\r\n$9\r\nsubscribe\r\n$1\r\nb\r\n:1\r\n
redis subscribe a b → two *3 frames
```

That sibling half belongs to `SGroup` (bug_86 class) — do **not** re-file it here, but a central fix must
cover both, and the fix agent should say explicitly in the commit which half it touches.

#### 89.5 — `CONFIRMED (reproduced)`

```
velo wait 0 0     → -ERR value is not an integer or out of range\r\n
velo wait 1 0     → -ERR value is not an integer or out of range\r\n
velo wait 1 -1    → -ERR value is not an integer or out of range\r\n
redis wait 0 0    → 1
redis wait 1 0    → 1
redis wait 1 -1   → ERR timeout is negative
redis wait -1 1000 → 1
```

`WGroup.java:80-82` rejects every non-positive value with `ErrorReply.INVALID_INTEGER`. Redis 7.2 accepts
`numreplicas <= 0` (the first-try path `ackreplicas >= numreplicas` is trivially true,
`replication.c:3553-3558`), accepts `timeout == 0` as "no timeout" (`timeout.c:161-195`, only `tval < 0`
errors, with the distinct text `timeout is negative`), and even accepts a negative `numreplicas`.
The agent-1 statement that `WAIT 1 0` "polls until satisfied" is **not** what redis does with a timeout of 0
and a satisfied condition — redis answers immediately through the first-try path; with an *unsatisfied*
condition redis blocks forever. The fix direction in the finding is therefore sound, but the acceptance
criteria must be stated as: `timeout < 0` → `-ERR timeout is negative`; `timeout == 0` → block until
satisfied (no deadline); `numreplicas <= 0` → answer immediately with the current ack count.

The two existing test rows at `src/test/groovy/io/velo/command/WGroupTest.groovy:136-144` encode the
divergent behaviour and must be updated by the fix (they are the wrong oracle, exactly as agent 1 says).

#### 89.6 — `CONFIRMED (reproduced)`

Temp spec (`review 89.6 89.7 wait on write-less client and on a slave`): a fresh socket with no write gets
`IntegerReply.REPLY_0` from `wait 1 100` (`WGroup.java:88-92`). Live redis, fresh connection, one online
replica: `wait 0 0` → `1`, i.e. `replicationCountAcksByOffset(0)` counts every `SLAVE_STATE_ONLINE` replica
(`replication.c:3503-3516`); a fresh client's `c->woff` is 0 (`server.c:3690-3696` only advances it on a
propagated write). Redis therefore uses WAIT-without-a-prior-write as a replication liveness probe, and
velo reports 0. Confirmed. The suggested fix (run the count logic with `lastSetSeq = 0`) is the right shape,
but note the per-slot architectural divergence already recorded in the non-findings: the replicas counted
must be the ones of the slot the client last wrote to, and for a write-less client there is no such slot —
the fix agent must pick a well-defined slot (e.g. the slot the command is dispatched to) and document it.

#### 89.7 — `CONFIRMED (reproduced)`

Temp spec: with `ReplPairTest.mockAsSlave()` in the slot's `replPairs` and `lastSetSeq != 0`,
`wait 1 100` → `IntegerReply.REPLY_0` (`WGroup.java:94-98`). Live redis on the replica → `ERR WAIT cannot
be used with replica instances. ...`. Confirmed as a protocol divergence; severity **Low** is right (error
text only, but it removes the client's ability to distinguish "I am a replica" from "0 replicas acked").

Note for the fix: velo's replica role is **per slot**, so the check can only consult the `lastSetSlot`
(`WGroup.java:94`); a client that never wrote has no `lastSetSlot` and the check is skipped — that is
consistent with the 89.6 fix (the "no prior write" case is answered before the slot is known).

#### 89.8 — `CONFIRMED (reproduced)`

```
velo wait 1 60001 → -ERR timeout should <= 60000\r\n
redis wait 1 120000 → 1   (answered in 1 ms, no cap)
```

`WGroup.java:83-86` applies a 60 s cap with a text that matches no redis message. Redis only rejects a
negative timeout and a value that overflows `LLONG_MAX - now` (`timeout.c:180-195`). Confirmed; severity
**Low** is right.

The fix direction is accepted: accept any non-negative timeout. Note that the 60 s cap is what makes the
current implementation tolerable at all (it bounds the async wait); once 89.1's polling loop lands, the
timeout must be a real deadline, not a hard rejection.

#### 89.9 — `REFUTED`

The finding is correct about the code (`WGroup.java:67-69` returns `ErrorReply.FORMAT` for
`data.length != 3`) but wrong about the protocol. `ErrorReply.FORMAT` is an internal **sentinel**, not a
wire text: the net layer rewrites it to the redis arity error before writing
(`MultiWorkerServer.java:611-615`, `:674-678` for the synchronous paths, `:702-704` for the async path):

```java
if (reply == ErrorReply.FORMAT) {
    reply = ErrorReply.WRONG_NUMBER(cmd);
}
```

Live wire capture from the real velo server:

```
velo wait        → -ERR wrong number of arguments for 'wait' command\r\n
velo wait 1      → -ERR wrong number of arguments for 'wait' command\r\n
velo wait 1 2 3  → -ERR wrong number of arguments for 'wait' command\r\n
```

which is byte-identical to redis' `ERR wrong number of arguments for 'wait' command`. The repo already
documents this convention at `src/test/groovy/io/velo/command/TGroupTest.groovy:71` ("redis answers wrong
number of arguments for 'time', the net layer rewrites FORMAT to that text").

The finding's parenthetical claim that `ErrorReply.WRONG_NUMBER` has a stray `*` prefix is also stale: the
current constant produces `-ERR wrong number of arguments for 'test' command\r\n`
(`src/test/groovy/io/velo/reply/RESPReplyTest.groovy:58-59`).

Disposition: **no change needed** for 89.9. Do not change `WGroup.java:67-69` — returning `ErrorReply.FORMAT`
there is the velo-wide convention. One caveat for the fix agent: any *new* error text introduced by the
89.2/89.5/89.7/89.8 fixes must NOT be `ErrorReply.FORMAT`, otherwise the net layer will rewrite it into
"wrong number of arguments for '<cmd>' command" and mask the intended message.

### Additional observations

- **`SGroup.subscribe()` has the same two defects** (single `*3N` frame instead of N frames, and a
  channel-wide socket count instead of the client's own count) — visible in the live-velo capture
  (`velo subscribe a b → *6 ...`). This is the `SGroup` half of bug_86's class and is **not** re-filed here;
  a fix for 89.4 that introduces a multi-frame reply should cover it, and the commit message must say
  which half it fixes.
- **`ErrorReply.FORMAT` is overloaded across the codebase** — it means both "wrong arity" and (in several
  commands) "bad value". Because the net layer rewrites it unconditionally, every command that returns it
  answers with an arity error on the wire. That is a pre-existing velo-wide convention, not a bug of this
  round, but it is the reason 89.2's error text differs from what agent 1 reported.
- **`IntegerReply` has no `equals()`** (`src/main/java/io/velo/reply/IntegerReply.java`) — a regression test
  must compare `getInteger()`, not `new IntegerReply(x) == reply`; the fix agent's tests should use
  `getInteger()`.
- **`OneSlot.delayRun()` is a no-op when `slotWorkerEventloop == null`** (`OneSlot.java:757-764`), so the
  89.1 polling loop must not rely on `delayRun` for its unit test — the test must set
  `oneSlot.slotWorkerEventloop` to a live event loop, exactly as `WGroupTest.groovy:68-77` does.
- **`ErrorReply.WRONG_NUMBER` is already correct** — see 89.9; do not "repair" it.

### Recommended fix order (for the fix agent)

1. **89.5 + 89.8 + 89.7 + 89.9-adjacent** — the WAIT argument parsing and the replica error. Small, local
   changes in `WGroup.cmdWait()`; they unblock the regression-test rewrite of `WGroupTest.groovy` and remove
   the two wrong-oracle rows (lines 136-144) that currently encode the divergent behaviour. One bug per
   commit, but they are adjacent enough to be done back-to-back.
2. **89.1** — the WAIT latency fix (immediate first-try + polling). This is the highest-severity finding and
   the only one that changes the shape of the reply (sync vs async). It must land after 89.5/89.8, because
   `WAIT 0 t` and `WAIT n 0` (89.5) depend on the new first-try path.
3. **89.6** — the write-less-client count. Depends on 89.1's first-try path (the count must be computed in
   the same place) and on the slot-selection decision recorded above.
4. **89.3 + 89.2** — the per-socket subscription view in `SocketInspector`. They share one piece of
   infrastructure (a per-socket set of `ChannelAndIsPattern`), so implement the view first as its own
   commit, then 89.3, then 89.2 on top of it.
5. **89.4** — the multi-frame reply. Largest change (reply model + write path), and it also affects
   `SGroup.subscribe()`. Do it last, or bundle it explicitly with the `SGroup` half in one commit with an
   explicit note (the skill requires one bug per commit; if the two halves must land together, state that
   in the commit message and in the Fix Log).

Each bug follows TDD: rebuild the temp spec of Appendix A as a permanent failing regression test first,
watch it fail, then implement the minimal fix, then `./gradlew :test --tests "io.velo.command.WGroupTest"`
(and `UGroupTest`) green, then confirm the changed lines/branches with
`python3 scripts/jacoco_cover.py io.velo.command.WGroup <start> <end>`.

## Response (AI agent 1, round 1)

All 9 verdicts accepted. I re-verified the one refutation (89.9) and the refinements against the codebase before
responding — no verdict is accepted on faith. No disagreement is filed; the retraction of my own finding below is
backed by fresh citations.

### 89.9 — retracted, the refutation is verified

I reproduced agent 2's chain of evidence independently:

- `MultiWorkerServer.java:612-614` (sync, reuse-net-worker path), `:675-677` (sync, other-eventloop path) and
  `:702-704` (`transferAsyncReply`, the async path WAIT's `AsyncReply` takes) all contain
  `if (reply == ErrorReply.FORMAT) { reply = ErrorReply.WRONG_NUMBER(cmd); }` **before** `reply.buffer()` is
  written — the rewrite is reference-equality on the shared sentinel, and `WGroup.cmdWait()` returns exactly that
  shared instance, so every WAIT arity error reaches the wire as
  `-ERR wrong number of arguments for 'wait' command`, matching the reviewer's live capture and Redis.
- The only two sites that return `ErrorReply.FORMAT.buffer()` **without** the rewrite are out of this round's
  scope: `MultiWorkerServer.java:424` (`handleFast`, ECHO wrong arity only — `wait` never routes there, the
  switch at 419-435 handles only ECHO/PING) and `:461` (SHUTDOWN invalid args). The ECHO one is a pre-existing
  CGroup-scope divergence (bug_73 already tracks PING/ECHO handleFast quirks as an open follow-up), not a U/W
  command and not re-filed here.
- My parenthetical "stray `*` prefix in `ErrorReply.WRONG_NUMBER`" was stale: it was true when bug_83 filed it
  (report line 920: the message was built as `"*wrong number of arguments..."`), and commit `7487c439`
  ("feat: add msetnx command. change error reply content when wrong number.") has since rebuilt it as
  `wrong number of arguments for '<cmd>' command` (ErrorReply.java:20-22), now locked by
  `RESPReplyTest.groovy:58-59`. I had carried the residual note forward without re-checking the constant — my
  mistake, and the reason I cited a dead follow-up.

Finding 89.9 is retracted (title struck through above, evidence kept). Disposition: **no change needed**; do not
touch `WGroup.java:67-69`.

### 89.1-89.8 — all `CONFIRMED (reproduced)` verdicts accepted

No pushback on any of them; the temp spec + live differential + wire captures match my static analysis. The
refinements are accepted and folded into the fix contracts:

- **89.1**: accepted. The two refinements are correct and matter: (a) the first-try check must run **before**
  the `SettablePromise`/`AsyncReply` is created so the satisfied case is a sync reply; (b) each poll tick must
  re-read `oneSlot.getSlaveReplPairListSelfAsMaster()` (the existing `WGroup.java:111` behaviour), not cache the
  list captured at entry.
- **89.2**: accepted, including the error-text correction. My report described the sentinel
  (`ErrorReply.FORMAT`), not the wire text; on the wire the client sees
  `-ERR wrong number of arguments for 'unsubscribe' command` (same rewrite chain as 89.9). The finding stands
  unchanged in substance: bare `UNSUBSCRIBE`/`PUNSUBSCRIBE` must unsubscribe-all (or reply the nil-channel
  triple), not error. Caveat adopted: the fix must **not** return `ErrorReply.FORMAT` for any non-arity error —
  the net layer would rewrite it into an arity error and mask the intended message.
- **89.3**: accepted. Per-socket subscription view confirmed missing (`SocketInspector.java:458` keeps only
  channel→sockets); the fix adds one and the reply uses this client's remaining count, split by type
  (channels vs patterns) as `clientSubscriptionsCount(c, type)` does (pubsub.c:167).
- **89.4**: accepted, including the instruction not to re-file the `SGroup.subscribe()` sibling (bug_86 class)
  and to state in the commit/Fix Log which half a multi-frame fix covers.
- **89.5**: accepted with the reviewer's acceptance criteria verbatim: `timeout < 0` →
  `-ERR timeout is negative`; `timeout == 0` → no deadline (block until satisfied); `numreplicas <= 0` →
  immediate sync answer with the current ack count. My checklist line "WAIT 1 0 polls until satisfied" was about
  the unsatisfied case; the reviewer's phrasing is the accurate contract. The two wrong-oracle rows at
  `WGroupTest.groovy:136-144` get rewritten by this fix.
- **89.6**: accepted. Verification note on the reviewer's slot remark: `lastSetSlot` defaults to `0`
  (`VeloUserDataInSocket.java:77`), so a write-less client has a *de facto* slot 0, not "no slot" — the fix must
  nevertheless pick and document a well-defined slot (or count across slots) rather than rely on the field's
  default. Sequencing note for the fix agent: doing 89.5 before 89.1 (the reviewer's order) means 89.5's
  `numreplicas <= 0` immediate answer must already compute the ack count synchronously — extract that count
  helper once and let 89.1's first-try reuse it, so the two commits do not duplicate the logic.
- **89.7**: accepted. The per-slot caveat is inherent to velo's model; the error is returned for the
  `lastSetSlot` only, consistent with 89.6.
- **89.8**: accepted, including that the cap's removal is safe only once 89.1 makes the timeout a real poll
  deadline (today the cap bounds an otherwise unconditional full sleep).

### Additional observations — verified

- `IntegerReply` has no `equals()` override (checked `src/main/java/io/velo/reply/IntegerReply.java` — no
  `equals` in the file); the existing `WGroupTest` comparisons against `IntegerReply.REPLY_0` only work because
  of the shared constant. New regression tests must compare `getInteger()`.
- `OneSlot.delayRun()` no-op without `slotWorkerEventloop` confirmed earlier (`OneSlot.java:757-764`); tests for
  the poll loop must wire a live event loop as `WGroupTest.groovy:68-77` does.
- One nuance to the "FORMAT is overloaded" observation: the rewrite is **reference**-based, so only the exact
  shared `ErrorReply.FORMAT` instance is rewritten — a `new ErrorReply("syntax error")` would reach the wire
  verbatim. That makes the fix-agent caveat stronger: never reuse the sentinel for non-arity errors, and never
  allocate it fresh for arity errors.

### Disposition after response

8 findings proceed to the fix phase in the reviewer's order (89.5 → 89.8 → 89.7 → 89.1 → 89.6 → 89.3 → 89.2 →
89.4, one bug per commit, TDD + JaCoCo each); 89.9 retracted, no change needed. Nothing further is disputed.

## Fix Log (AI agent 1, round 1 fix phase)

Fix order as agreed in the Response: 89.5 → 89.8 → 89.7 → 89.1 → 89.6 → 89.3 → 89.2 → 89.4. Every bug: permanent
regression rows added to the single `def 'test wait'()` / `def 'test unsubscribe'()` methods first, watched RED
(failure reason cited below), minimal fix, focused tests green, JaCoCo confirmed on the changed lines/branches.

| Bug | Failing test evidence | Fix commit | Focused test | JaCoCo |
|---|---|---|---|---|
| 89.5 | RED `WGroupTest.'test wait'` `ConditionNotSatisfiedError at WGroupTest.groovy:140` — `reply instanceof IntegerReply` false: `wait 0 0` answered `ErrorReply.INVALID_INTEGER` (the two wrong-oracle rows 136-144 rewritten as the reviewer required) | `eee46598` `fix: wait accepts numreplicas<=0 answering the ack count, timeout 0, and rejects a negative timeout with the redis text` | `:test --tests "io.velo.command.WGroupTest"` 4/4 green | `WGroup.java` 76-84 (`Long.parseLong`, `timeout < 0` → `timeout is negative`, both branches), 111-112 (`numReplicas <= 0` sync answer, both branches), helper `countCaughtUpSlaves` 146-157 fully covered after the uncaught-third-slave row |
| 89.8 | RED `WGroupTest.'test wait'` at `WGroupTest.groovy:176` — `wait 0 60001` answered the cap error `timeout should <= 60000` | `65031dea` `fix: wait has no timeout cap, redis accepts any non-negative timeout` | same → green | guard region 66-110 fully covered, both branches of every check; cap block deleted |
| 89.7 | RED `WGroupTest.'test wait'` `SpockComparisonFailure at WGroupTest.groovy:99` — as-slave `wait 2 100` answered `IntegerReply.REPLY_0` instead of the redis error | `87550181` `fix: wait on a replica answers the redis replica error instead of 0` | same → green | `WGroup.java:95-97` both branches of `isAsSlave` covered; the error is a fresh `ErrorReply` (not the FORMAT sentinel, per the net-layer caveat) |
| 89.1 | RED `WGroupTest.'test wait'` at `WGroupTest.groovy:91` — no-slaves `wait 2 100` answered sync 0 instead of blocking (`AsyncReply` expected) | `11fc5087` `fix: wait answers immediately when replicas already acked, polls with 10ms ticks otherwise, timeout 0 has no deadline` | same → green (incl. new rows: satisfied wait sync `< 90 ms`, early-wake `< 290 ms` of a 300 ms timeout after a +50 ms catch-up, no-deadline `wait 4 0` settles only when the 4th slave catches up) | first-try 108-110 (both branches), poller `pollWaitForReplicas` 133-151: satisfied / deadline / no-deadline / re-poll branches all exercised; line 144 shows 3 of 4 condition combinations — the 4th (`timeout <= 0` AND elapsed evaluated) is unreachable by short-circuit; the pre-async empty-list early return was **removed** (Redis blocks until the deadline — row updated accordingly) |
| 89.6 | RED `WGroupTest.'test wait'` at `WGroupTest.groovy:86` — write-less client `wait 1 100` answered 0 with one replica connected | `352c5d50` `fix: wait from a write-less client counts the distinct replicas like redis offset 0` | same → green | `lastSetSeq == 0` both branches; `countDistinctReplicas()` 179-186 both loops both directions |
| 89.3 | RED `UGroupTest.'test unsubscribe'` at `UGroupTest.groovy:81` — count 1 (channel sockets) instead of 2 (client's remaining b, c) | `490a627b` `fix: unsubscribe confirmation count is the client remaining subscriptions, not the channel socket count` (+ test-only `4e8ec8b2` for the pattern-exclusion branch) | `UGroupTest`, `SocketInspectorTest`, `PGroupTest`, `SGroupTest` all green (two `SocketInspectorTest` rows updated to the redis oracle: not-subscribed channel answers the client's own remaining count) | `SocketInspector.unsubscribe()` 576-601 all branches incl. pattern exclusion and never-subscribed socket; per-socket view maintained in `subscribe()`/`onDisconnect()`/`clearAll()` |
| 89.2 | RED `UGroupTest.'test unsubscribe'` at `UGroupTest.groovy:108` — bare `unsubscribe` answered `ErrorReply.FORMAT` instead of one confirmation per channel | `97440cc4` `fix: no-argument unsubscribe unsubscribes all and answers the nil channel confirmation when none` | `UGroupTest`, `SocketInspectorTest` green (`'test handle'` restructured: the FORMAT row was the bug oracle; new direct rows for `SocketInspector.subscriptionsOf()` per the new-public-method rule) | no-arg both branches (per-channel triples / nil-channel triple), `subscriptionsOf()` 608-629 fully covered (null socket, fresh socket, channel vs pattern type) |
| 89.4 | RED `UGroupTest.'test unsubscribe'` at `UGroupTest.groovy:116` — `unsubscribe a b` answered one `MultiBulkReply` of 6 elements instead of `MultiFrameReply` | `a59c1bd6` `fix: multi-channel unsubscribe replies one resp frame per channel, not one flattened 3n array` | `UGroupTest`, `PGroupTest` (single-channel unchanged), `SGroupTest`, `MultiFrameReplyTest`, `RESPReplyTest` green; wire-level assertion: two `*3` frames byte-identical to the redis capture | new `io.velo.reply.MultiFrameReply` 20/20 lines covered by its own spec (`MultiFrameReplyTest`: resp2 concat, resp3 per-frame delegation, http fold, single/empty) + the UGroup byte-level row; `UGroup.unsubscribe` 86-100 all branches (incl. the single-frame passthrough) |

Notes for the re-review:

- **89.5 before 89.1**: as agreed, `numReplicas <= 0` introduced the sync count first (`eee46598`); 89.1's first-try then
  reused the same `countCaughtUpSlaves()` helper — no duplication. The `timeout == 0` no-deadline semantics were
  completed by 89.1's poller (`11fc5087`), exactly per the reviewer's cross-fix acceptance criteria
  (`timeout < 0` → `-ERR timeout is negative`; `timeout == 0` → no deadline; `numreplicas <= 0` → immediate count).
- **89.3's `SocketInspectorTest` row updates**: two rows (`n2 == 0` → `1`, `n == 0` → `1`) encoded the old
  channel-socket-count semantics; under the redis oracle they must answer the client's remaining count — updated in
  the same commit, not weakened.
- **`IntegerReply.REPLY_0`/`REPLY_1` are anonymous `Reply` implementations without `getInteger()`** — discovered when
  the nil-triple used the constant and the cast failed (`GroovyCastException IntegerReply$2`); the nil-triple now uses
  `new IntegerReply(0)`. Worth knowing for future test assertions.
- Final regression at HEAD: `UGroupTest`, `WGroupTest`, `PGroupTest`, `SGroupTest`, `SocketInspectorTest`,
  `MultiFrameReplyTest`, `RESPReplyTest`, `CGroupTest` — all green in one enumerated run.
- **Live e2e differential at HEAD `a59c1bd6`** (agent 1, evidence log
  `logs/20261118_152500_ugroup_wgroup_e2e_agent1_bug_89.log`): the built `velo-1.0.0.jar` on port 7379 vs the live
  Redis 7.2.11 master+replica on 7788/7789, redis-cli + raw-socket probes. Results: 89.9 arity errors byte-identical;
  89.5 zero/negative numreplicas answered with integers, `timeout is negative` identical; 89.8 `wait 0 120000`
  answered in 4 ms (no cap error); 89.1 `SET; WAIT 1 1000` on one connection blocked the full 1016 ms then answered 0
  (unsatisfiable write client — the removed early return), redis' satisfied case answered in 2 ms; 89.6 fresh-connection
  wait answered the replica count synchronously; 89.2/89.3/89.4 raw-wire bytes identical to redis for every
  unsubscribe case (nil-channel triple, per-client counts incl. the two-client case, two `*3` frames for
  `UNSUBSCRIBE x y`). The **only** divergence in the sweep is `subscribe a b` (`*6` flattened, channel-socket
  counts) — the recorded `SGroup.subscribe` residual (Residual Risk #1), untouched by design.

## Re-review (AI agent 2)

**Method:** per-commit diff review of the 9 fix commits (`eee46598` … `a59c1bd6`, HEAD `a59c1bd6`);
**RED independently re-derived for every bug** by restoring the commit's parent production code while keeping the
commit's own test file (the Gradle Groovy worker intermittently dies with "Java heap space" — the re-runs were
retried, that is a build-environment flake, not a code failure); the focused test classes re-run at HEAD; JaCoCo
re-inspected with `scripts/jacoco_cover.py`; and a **live wire differential** of the freshly built
`build/libs/velo-1.0.0.jar` (HEAD) against a live Redis 7.2.11 (master `:7798` + replica `:7799`, and a
replica-less master) probed with raw sockets. Evidence log:
`doc/bug_reviews/logs/20261118_162000_ugroup_wgroup_rereview_agent2_bug_89.log`.

### RED re-verification (parent production code + the commit's own test file)

| Bug | Commit | Re-produced RED failure | Fix Log quote | Match |
|---|---|---|---|---|
| 89.5 | `eee46598` | `WGroupTest > test wait FAILED — ConditionNotSatisfiedError at WGroupTest.groovy:140` | "ConditionNotSatisfiedError at WGroupTest.groovy:140" | ✅ identical |
| 89.8 | `65031dea` | `WGroupTest > test wait FAILED — ConditionNotSatisfiedError at WGroupTest.groovy:176` | "at `WGroupTest.groovy:176`" | ✅ identical |
| 89.7 | `87550181` | `WGroupTest > test wait FAILED — SpockComparisonFailure at WGroupTest.groovy:99` | "SpockComparisonFailure at `WGroupTest.groovy:99`" | ✅ identical |
| 89.1 | `11fc5087` | `WGroupTest > test wait FAILED — ConditionNotSatisfiedError at WGroupTest.groovy:91` | "at `WGroupTest.groovy:91`" | ✅ identical |
| 89.6 | `352c5d50` | `WGroupTest > test wait FAILED — ConditionNotSatisfiedError at WGroupTest.groovy:86` | "at `WGroupTest.groovy:86`" | ✅ identical |
| 89.3 | `490a627b` | `UGroupTest > test unsubscribe FAILED — SpockComparisonFailure at UGroupTest.groovy:81` | "at `UGroupTest.groovy:81`" | ✅ identical |
| 89.2 | `97440cc4` | `UGroupTest > test handle FAILED — ConditionNotSatisfiedError at UGroupTest.groovy:48` and `test unsubscribe FAILED — ConditionNotSatisfiedError at UGroupTest.groovy:115` | "at `UGroupTest.groovy:108`" | ⚠️ same defect, **line differs** (108 is a blank line in the commit's test file; the actual failing assertions are 48 and 115) |
| 89.4 | `a59c1bd6` | `UGroupTest > test unsubscribe FAILED — ConditionNotSatisfiedError at UGroupTest.groovy:116` | "at `UGroupTest.groovy:116`" | ✅ identical |

`4e8ec8b2` is a test-only commit (the pattern-exclusion branch of the count) — no production change, nothing to
re-derive. **All 8 REDs independently re-derived**; one line-number citation in the Fix Log is off, the failure
shape is the one described.

### Focused tests at HEAD (`a59c1bd6`)

```
io.velo.command.WGroupTest         tests= 4  fail=0 err=0
io.velo.command.UGroupTest         tests= 3  fail=0 err=0
io.velo.SocketInspectorTest        tests=12  fail=0 err=0
io.velo.command.PGroupTest         tests=22  fail=0 err=0
io.velo.command.SGroupTest         tests=29  fail=0 err=0
io.velo.reply.MultiFrameReplyTest  tests= 2  fail=0 err=0
io.velo.reply.RESPReplyTest        tests= 4  fail=0 err=0
```
`./gradlew :test --tests "io.velo.command.WGroupTest" --tests "io.velo.command.UGroupTest" --tests
"io.velo.SocketInspectorTest" --tests "io.velo.reply.MultiFrameReplyTest" --tests "io.velo.command.PGroupTest"
--tests "io.velo.command.SGroupTest" --tests "io.velo.reply.RESPReplyTest"` → **BUILD SUCCESSFUL**.

### JaCoCo at HEAD (`scripts/jacoco_cover.py`)

| Class | Range | Lines | Branches |
|---|---|---|---|
| `io.velo.command.WGroup` | 60-188 | 53/54 covered, 1 partial (line 147) | 12 of 13 branch lines fully covered |
| `io.velo.command.UGroup` | 55-102 | 21/21 covered | 8/8 branch lines fully covered |
| `io.velo.reply.MultiFrameReply` | 1-68 | 20/20 covered | 3/3 branch lines fully covered |
| `io.velo.SocketInspector` | 584-601 (`unsubscribe`) + 612-629 (`subscriptionsOf`) | 29/29 covered | 8/9 branch lines fully covered |

The single partial is `WGroup.java:147` — `if (timeoutMillis > 0 && System.currentTimeMillis() - beginMillis >= timeoutMillis)`:
the 4th condition combination (`timeout <= 0` **and** the elapsed check evaluated) is unreachable by
short-circuit — exactly as agent 1 declared. The 3 uncovered `SocketInspector` lines (560, 563, 564) are the
**pre-existing** `slotWorkerEventloopArray` fallback inside `subscribe()` (untouched by this round); the line this
round added there (570) is covered.

### Live wire differential at HEAD vs Redis 7.2.11

Redis 7.2.11 (master `:7798`, one online replica `:7799`) vs the built velo jar on `:41998` (single node, no
replica) — raw socket probes:

| Command | Redis 7.2.11 | Velo HEAD `a59c1bd6` | Verdict |
|---|---|---|---|
| `unsubscribe` (no arg, subscribed to nothing) | `*3 $11 unsubscribe $-1 :0` | `*3 $11 unsubscribe $-1 :0` | ✅ byte identical (89.2) |
| `unsubscribe a b` | `*3 …a :1` `*3 …b :0` (two frames) | `*3 …a :1` `*3 …b :0` (two frames) | ✅ byte identical (89.4) |
| A `unsubscribe x` while B holds `x` | `*3 $11 unsubscribe $1 x :0` | `*3 $11 unsubscribe $1 x :0` | ✅ byte identical (89.3) |
| `wait 0 0` | `:0` (no replica) / `:1` (1 replica) | `:0` | ✅ (89.5) |
| `wait 1 -1` | `-ERR timeout is negative` | `-ERR timeout is negative` | ✅ byte identical (89.5) |
| `wait -1 1000` | `:0` | `:0` | ✅ (89.5) |
| `wait` / `wait 1` | `-ERR wrong number of arguments for 'wait' command` | `-ERR wrong number of arguments for 'wait' command` | ✅ byte identical (89.9, REFUTED at round 1) |
| `wait 2 2000` (client **with** a prior write, no replica) | blocks 2.10 s → `:1` (1 replica) | blocks 2.01 s → `:0` | ✅ blocks the full timeout (89.1) |
| `wait 1 0` (client **with** a prior write, unsatisfiable) | blocks forever | blocks forever (`<timeout>` after 6 s) | ✅ no deadline (89.5/89.1) |
| `wait 2 2000` (client **without** a prior write) | blocks 2.10 s → `:1` | answers `:0` **immediately** | ❌ **new divergence — Follow-up 1** |
| `subscribe b` (client holds `a`, another client holds `a`) | `*3 $9 subscribe $1 b :2` (client count) | `*3 $9 subscribe $1 b :1` (channel socket count) | ❌ the SGroup half of 89.3 — Residual Risk #1, out of this round's scope |

### Per-commit verdicts

| Bug | Commit | Diff review | Test at HEAD | JaCoCo | Verdict |
|---|---|---|---|---|---|
| 89.5 | `eee46598` | `Long.parseLong` for both args; only `timeout < 0` rejected, with the exact redis text `timeout is negative`; `numReplicas <= 0` answers the current ack count synchronously (redis' first-try `ackreplicas >= numreplicas`); the `numReplicas <= 0` check is placed **before** the first-try, so `WAIT 0 t` answers the count and never blocks | green | covered (76-84, 111-112) | **ACCEPTED** |
| 89.8 | `65031dea` | the 60 s cap block deleted; the timeout is now a `long` deadline compared in the poller — no overflow, no cap | green | covered | **ACCEPTED** |
| 89.7 | `87550181` | the as-slave branch returns a fresh `ErrorReply` with the redis text (not the `FORMAT` sentinel — the reviewer's caveat respected) | green | covered (95-97) | **ACCEPTED** (with Follow-up 2 on the check order) |
| 89.1 | `11fc5087` | first-try before the promise (a satisfied wait answers **synchronously**), 10 ms poller re-reading the slave list on every tick, the deadline enforced inside the tick, `timeout == 0` → no deadline; the old "no slaves → immediate 0" early return was removed (redis blocks) | green | covered except the unreachable short-circuit combination | **ACCEPTED** (with Follow-up 3 on the dropped-tick hang) |
| 89.6 | `352c5d50` | `lastSetSeq == 0` → `countDistinctReplicas()` (distinct host:port of the slots this node masters) — the count now matches redis; the blocking semantics of the write-less case are **not** implemented | green | covered (86, 179-186) | **ACCEPTED for the reported defect** (Follow-up 1) |
| 89.3 | `490a627b` (+ `4e8ec8b2`) | a per-socket subscription view (`subscriptionsBySocket`) maintained in `subscribe()`/`onDisconnect()`/`clearAll()`; the count is now the client's remaining subscriptions **of the same type** (channels vs patterns), matching `clientSubscriptionsCount`; the two `SocketInspectorTest` rows that encoded the old channel-socket semantics were updated to the redis oracle, not weakened | green | covered | **ACCEPTED** |
| 89.2 | `97440cc4` | no-arg unsubscribe enumerates this client's subscriptions of the requested type and emits one confirmation per channel, or the `["unsubscribe", nil, 0]` triple when there are none; `PUNSUBSCRIBE` gets the same treatment through the same code path | green | covered | **ACCEPTED** |
| 89.4 | `a59c1bd6` | new `io.velo.reply.MultiFrameReply` concatenates complete RESP frames (RESP frames are self-delimiting → byte identical to writing them separately); the single-channel case keeps the plain `MultiBulkReply` triple; `bufferAsResp3()` delegates per frame and `bufferAsHttp()` folds CRLF like `MultiBulkReply` | green | 20/20 lines covered | **ACCEPTED** (the SGroup half stays open — Residual Risk #1) |

### Follow-ups (non-blocking, for the next round — none of them invalidates a fix)

1. **A WAIT from a client that never wrote never blocks.** `WGroup.java` answers the replica count
   synchronously in the `lastSetSeq == 0` branch, so `WAIT 2 2000` returns immediately with the current count,
   while redis blocks until the deadline and then answers the count (`replication.c:3553-3558` only skips the
   block when `ackreplicas >= numreplicas`; with `numreplicas > online replicas` redis blocks even for a
   write-less client). Live evidence: redis `wait 2 2000` → blocks 2.10 s → `:1`; velo → `:0` in 0.00 s.
   The client that never wrote has nothing to wait for, so the practical impact is limited to the
   "WAIT as a replication health probe" use case — but the semantics differ. Fix direction: route the write-less
   case through the same first-try + poller, with the offset treated as 0.
2. **The replica-role check is not first.** Redis checks `server.masterhost` before parsing the arguments
   (`replication.c:3542-3545`), so on a replica *every* `WAIT` answers the replica error. Velo checks the
   timeout first and the write-less case before the `isAsSlave()` check, so on a replica a write-less client
   gets the replica count and `WAIT 1 -1` gets `timeout is negative`. The per-slot model makes the role check
   slot-dependent, so the fix needs a decision on which slot to consult for a write-less client.
3. **`OneSlot.delayRun()` silently drops the task when `slotWorkerEventloop == null`** (`OneSlot.java:757-764`).
   The deadline of the poller is only checked *inside* the dropped task, so a slot without a worker event loop
   leaves the promise unsettled — the client hangs forever, timeout or not. Pre-existing shape (the old code had
   the same `delayRun` dependency), but the new poller made the timeout a real deadline that is enforced only
   inside the tick. Fix direction: settle the promise immediately when the slot has no event loop, or arm the
   deadline outside the event loop.
4. **`countDistinctReplicas()` counts configured replicas, not online ones.** `getSlaveReplPairListSelfAsMaster()`
   filters only `isSendBye()`, while redis counts `SLAVE_STATE_ONLINE` slaves only (`replication.c:3513`). A
   replica whose link is down is still counted by velo. The `WGroupTest` row "write-less client → 1" bakes this
   in (the mock replica never handshakes). Follow-up: filter on the link state.

### Residual Risk (agent 2 additions)

The residuals listed by agent 1 (Residual Risk #1-#6) all stand and are confirmed by this re-review; the
`SGroup.subscribe()` divergence was re-measured live at HEAD (`subscribe b` → velo `:1`, redis `:2`).
Agent 2 adds **Follow-ups 1-4** above (write-less WAIT never blocks; the replica check is not first; the
`delayRun` dropped-tick hang; `countDistinctReplicas` counts non-online replicas).

## Residual Risk

1. **`SGroup.subscribe()` still has both sibling defects** (one flattened `*3N` array, and the channel-socket count
   instead of the client's own count) — bug_86's class, deliberately not re-filed here per the reviewer's
   instruction. The central machinery now exists (`MultiFrameReply`, the per-socket subscription view in
   `SocketInspector`), so the SGroup half is a small follow-up: build one triple per channel, use
   a per-client count, return `MultiFrameReply`. Recommended as a bug_86 follow-up commit, not a new bug id.
   **→ FIXED after the re-review as bug_86 Follow-up F1, commit `12c68b49`** (RED→GREEN + JaCoCo + live wire
   parity; see the appended section in `bug_86_sgroup_commands_review_round_1.md`).
2. **WAIT polls every 10 ms** (`WGroup.pollWaitForReplicas`) instead of waking on the ack receipt like Redis
   (`processClientsWaitingReplicas` runs every event loop iteration). Worst-case added latency for an unsatisfied
   wait: one poll interval; the satisfied case is sync (first-try), so the common case matches Redis' latency.
3. **Write-less WAIT counts distinct replica endpoints across slots** (`countDistinctReplicas`) — the documented
   per-slot-model decision from the Response: a client with no writes has no `lastSetSlot`, so the closest analog of
   Redis' "number of online replicas" is the distinct host:port set of the slots this node masters. Divergences kept:
   handshaking pairs are counted (Redis counts `SLAVE_STATE_ONLINE` only), and a slave replicating N slots counts
   once (Redis would count 1 connection — equal).
4. **A `WAIT n 0` with unsatisfiable n never settles** (no deadline, matching Redis "block forever"). In Velo the
   client connection itself is not blocked — only the reply pends on the slot event loop (10 ms tick), so a slow
   poller per waiting client remains; Redis blocks the client similarly. If this becomes a concern, a server-side
   cancellation on client disconnect would be the fix (not implemented, no finding).
5. **`IntegerReply.REPLY_0` / `REPLY_1` are anonymous `Reply` implementations** without `getInteger()` and without
   being `IntegerReply` instances — a test-assertion footgun discovered during 89.2 (see Fix Log notes). Pre-existing,
   not touched.
6. The pre-existing bug_84 residuals touching the same `SocketInspector` code paths stand: the empty-map entry leak
   in `unsubscribe()` (`computeIfAbsent`) still lists subscriber-less channels in `PUBSUB CHANNELS`, and the glob
   matcher is still Java's.

## Summary

| # | Finding | Severity | Class | Verdict | Disposition |
|---|---|---|---|---|---|
| 89.1 | WAIT always sleeps the full timeout, never replies early | High | Correctness / Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`11fc5087`) |
| 89.2 | No-arg UNSUBSCRIBE answers an error instead of unsubscribing all | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` (error text refined) | fixed (`97440cc4`) |
| 89.3 | Unsubscribe confirmation count is channel-socket count, not per-client remaining | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`490a627b` + `4e8ec8b2`) |
| 89.4 | Multi-channel UNSUBSCRIBE returns one 3N array instead of N 3-element replies | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`a59c1bd6`) |
| 89.5 | WAIT 0 t / WAIT n 0 rejected with INVALID_INTEGER | Medium | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`eee46598`) |
| 89.6 | WAIT from a write-less client answers 0, Redis counts online replicas | Medium | Correctness / Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`352c5d50`) |
| 89.7 | WAIT on a replica answers 0 instead of the Redis error | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`87550181`) |
| 89.8 | 60 s WAIT timeout cap with a non-Redis error | Low | Redis protocol compatibility | `CONFIRMED (reproduced)` | fixed (`65031dea`) |
| 89.9 | Wrong-arity WAIT error text | Low | Redis protocol compatibility | `REFUTED` | no change needed (retracted by agent 1, stage 3) |

**Round 1 verification totals:** 8 `CONFIRMED (reproduced)`, 1 `REFUTED`, 0 `PARTIALLY CONFIRMED`,
0 `NOT A BUG`. *(Superseded by the Fix Log above: all 8 confirmed findings were fixed in the agreed order,
one bug per commit; 89.9 needed no change.)*

**Stage 5 re-review (agent 2):** all 8 fixes **ACCEPTED** — 8 REDs independently re-derived, the focused tests
green at HEAD `a59c1bd6`, the changed lines/branches JaCoCo-confirmed, and the fixed behaviours byte-identical to
Redis 7.2.11 on the wire. 4 non-blocking follow-ups are recorded in the Re-review section (write-less WAIT never
blocks; the replica-role check is not first; the `delayRun` dropped-tick hang; `countDistinctReplicas` counts
non-online replicas) and the `SGroup.subscribe()` sibling half stays open as a bug_86 follow-up
(Residual Risk #1).

## Appendix A — temporary review spec (removed after review)

The spec below was created under `src/test/groovy/io/velo/command/TempBug89ReviewTest.groovy`, run with
`./gradlew :test --tests "io.velo.command.TempBug89ReviewTest"` (4 specs, all green — it documents the
*current, buggy* behaviour), and then deleted. The fix agent should rebuild the relevant parts as permanent
regression tests (red first).

```groovy
package io.velo.command

import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.SocketInspector
import io.velo.SocketInspectorTest
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.ReplPairTest
import io.velo.reply.AsyncReply
import io.velo.reply.ErrorReply
import io.velo.reply.MultiBulkReply
import spock.lang.Specification

import java.time.Duration

class TempBug89ReviewTest extends Specification {
    final short slot = 0

    def 'review 89.1 wait never replies early'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def socket = SocketInspectorTest.mockTcpSocket()
        def wGroup = new WGroup(null, null, socket)
        wGroup.byPassGetSet = inMemoryGetSet
        wGroup.from(BaseCommand.mockAGroup())

        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start { eventloop.run() }
        Thread.sleep(100)
        localPersist.fixSlotThreadId(slot, eventloop.eventloopThread.threadId())
        oneSlot.slotWorkerEventloop = eventloop

        and:
        SocketInspector.updateLastSetSeq(socket, 1L, slot)
        def replPairAsMaster = ReplPairTest.mockAsMaster()
        replPairAsMaster.slaveCatchUpLastSeq = 1L
        oneSlot.replPairs.add(replPairAsMaster)

        when: 'the slave is already caught up, redis replies immediately'
        long start = System.currentTimeMillis()
        def reply = wGroup.execute('wait 1 5000')
        def promise = (reply as AsyncReply).settablePromise
        def settledAt = -1L
        while (!promise.isComplete()) {
            Thread.sleep(20)
        }
        settledAt = System.currentTimeMillis() - start
        println "89.1 settle elapsed millis = ${settledAt}"

        then: 'velo waits the full timeout instead of replying immediately'
        settledAt >= 4000
        reply instanceof AsyncReply

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'review 89.2 89.3 89.4 unsubscribe'() {
        given:
        def inspector = new SocketInspector()
        LocalPersist.instance.socketInspector = inspector
        def netWorkerEventloop = Eventloop.builder()
                .withThreadName('bug89-net-worker')
                .withIdleInterval(Duration.ofMillis(50))
                .build()
        netWorkerEventloop.keepAlive(true)
        Thread.start { netWorkerEventloop.run() }
        Thread.sleep(200)
        inspector.initByNetWorkerEventloopArray([netWorkerEventloop] as Eventloop[], [netWorkerEventloop] as Eventloop[])

        and:
        def socket = SocketInspectorTest.mockTcpSocket()
        def otherSocket = SocketInspectorTest.mockTcpSocket()
        def uGroup = new UGroup('unsubscribe', null, socket)
        uGroup.from(BaseCommand.mockAGroup())

        expect: '89.2 no-arg unsubscribe is rejected'
        uGroup.execute('unsubscribe') == ErrorReply.FORMAT

        when:
        inspector.subscribe('a', false, socket)
        inspector.subscribe('a', false, otherSocket)
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        uGroup.data = data2
        def reply = uGroup.unsubscribe(false)
        then: '89.3 the count is the channel socket count, not the client remaining count'
        // this client had only 'a', so redis answers 0, velo answers 1 (the other socket is still there)
        (reply as MultiBulkReply).replies[2].getInteger() == 1L
        println "89.3 count = ${(reply as MultiBulkReply).replies[2].getInteger()}"

        when:
        def data2b = new byte[2][]
        data2b[1] = 'b'.bytes
        uGroup.data = data2b
        def reply2 = uGroup.unsubscribe(false)
        then:
        (reply2 as MultiBulkReply).replies[2].getInteger() == 0L

        when:
        def data2c = new byte[3][]
        data2c[1] = 'a'.bytes
        data2c[2] = 'b'.bytes
        uGroup.data = data2c
        def reply3 = uGroup.unsubscribe(false)
        then: '89.4 multi channel unsubscribe is one array of 6 elements'
        reply3 instanceof MultiBulkReply
        (reply3 as MultiBulkReply).replies.length == 6
        println "89.4 reply = ${(reply3 as MultiBulkReply).replies*.toString()}"

        cleanup:
        netWorkerEventloop.breakEventloop()
        LocalPersist.instance.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'review 89.5 89.6 89.7 89.8 89.9 wait argument handling'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def socket = SocketInspectorTest.mockTcpSocket()
        def wGroup = new WGroup(null, null, socket)
        wGroup.byPassGetSet = inMemoryGetSet
        wGroup.from(BaseCommand.mockAGroup())

        expect: '89.9 wrong arity'
        wGroup.execute('wait') == ErrorReply.FORMAT
        wGroup.execute('wait 1') == ErrorReply.FORMAT
        wGroup.execute('wait 1 2 3') == ErrorReply.FORMAT

        and: '89.5 zero numreplicas / zero timeout rejected'
        wGroup.execute('wait 0 0') == ErrorReply.INVALID_INTEGER
        wGroup.execute('wait 1 0') == ErrorReply.INVALID_INTEGER

        and: '89.8 the 60s cap'
        def overCap = wGroup.execute('wait 1 60001')
        overCap instanceof ErrorReply
        println "89.8 error = ${(overCap as ErrorReply).getMessage()}"

        and: 'negative timeout uses the generic integer error, redis says "timeout is negative"'
        wGroup.execute('wait 1 -1') == ErrorReply.INVALID_INTEGER

        cleanup:
        LocalPersist.instance.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'review 89.6 89.7 wait on write-less client and on a slave'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def socket = SocketInspectorTest.mockTcpSocket()
        def wGroup = new WGroup(null, null, socket)
        wGroup.byPassGetSet = inMemoryGetSet
        wGroup.from(BaseCommand.mockAGroup())

        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start { eventloop.run() }
        Thread.sleep(100)
        localPersist.fixSlotThreadId(slot, eventloop.eventloopThread.threadId())
        oneSlot.slotWorkerEventloop = eventloop

        when: '89.6 a client that never wrote anything'
        def reply = wGroup.execute('wait 1 100')
        then:
        reply == IntegerReply.REPLY_0

        when: '89.7 the slot acts as slave'
        SocketInspector.updateLastSetSeq(socket, 1L, slot)
        oneSlot.replPairs.add(ReplPairTest.mockAsSlave())
        def reply2 = wGroup.execute('wait 1 100')
        then:
        reply2 == IntegerReply.REPLY_0

        cleanup:
        eventloop.breakEventloop()
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
```
