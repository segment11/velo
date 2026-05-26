# Replication Module Bug Review (Round 3)

Date: 2026-04-20

## Bug 1: REPL transport accepts unbounded frame lengths and copies partial payloads repeatedly

**Severity:** High
**Files:** `src/main/java/io/velo/repl/Repl.java:216-227`, `src/main/java/io/velo/decode/ReplDecoder.java:34-68`, `src/main/java/io/velo/repl/ReplRequest.java:117-127`

```java
var dataLength = buf.readInt();
if (dataLength <= 0) {
    throw new IllegalArgumentException("Repl content length should be positive");
}
```

```java
if (toFullyReadRequest != null) {
    var leftN = toFullyReadRequest.leftToRead();
    var canReadN = buf.readableBytes();
    ...
    toFullyReadRequest.nextRead(buf, canReadN);
}
```

```java
var dataExtend = new byte[data.length + n];
System.arraycopy(data, 0, dataExtend, 0, data.length);
nettyBuf.readBytes(dataExtend, data.length, n);
data = dataExtend;
```

`Repl.decode()` accepts any positive `dataLength` with no upper bound. `ReplDecoder` then keeps partially read requests alive until the full payload arrives, and `ReplRequest.nextRead()` reallocates and copies the entire accumulated byte array on every chunk. A malformed or hostile peer can advertise a huge frame and stream it slowly, causing unbounded heap growth and repeated O(n) copies while the connection stays open.

This is not mitigated by `RequestDecoder`: REPL traffic on the slave client side uses `ReplDecoder`, not the generic request path.

---

## Bug 2: IPv6 literal listen addresses break replication handshake and failover publishing

**Severity:** High
**Files:** `src/main/java/io/velo/repl/ReplPair.java:104-124`, `src/main/java/io/velo/command/XGroup.java:359`, `src/main/java/io/velo/repl/LeaderSelector.java:343-344`

```java
var separatorIndex = hostAndPort.indexOf(':');
if (separatorIndex <= 0 || separatorIndex != hostAndPort.lastIndexOf(':')
        || separatorIndex == hostAndPort.length() - 1) {
    throw new IllegalArgumentException("Invalid host:port format: " + hostAndPort);
}
```

```java
var hostAndPort = ReplPair.parseHostAndPort(netListenAddresses);
```

```java
var oldMasterHostAndPort = ReplPair.parseHostAndPort(leaderSelector.lastGetMasterListenAddressAsSlave);
var selfAsMasterHostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses);
```

`parseHostAndPort()` rejects any address containing more than one `:`. That means valid IPv6 literals such as `[2001:db8::1]:7379` or raw literal forms cannot be parsed. The breakage is live in at least two places:

- master-side `hello` handling, where the slave advertises its listen address
- leader failover publishing, where the old and new master addresses are parsed before the switch message is broadcast

As a result, replication and failover control paths are not IPv6-safe.

---

## Bug 3: `dyn_config` binlog type is registered but still completely unimplemented

**Severity:** Critical (latent)
**Files:** `src/main/java/io/velo/repl/BinlogContent.java:12-13,34-43`, `src/main/java/io/velo/repl/incremental/XDynConfig.java:16-31`

```java
dict((byte) 100), skip_apply((byte) 110), update_seq((byte) 120),
dyn_config((byte) 121), flush(Byte.MAX_VALUE);
```

```java
case dyn_config -> XDynConfig.decodeFrom(buffer);
```

```java
public int encodedLength() {
    throw new UnsupportedOperationException("XDynConfig is not implemented yet");
}

public static XDynConfig decodeFrom(ByteBuffer buffer) {
    throw new UnsupportedOperationException("XDynConfig is not implemented yet");
}

public void apply(short slot, ReplPair replPair) {
    throw new UnsupportedOperationException("XDynConfig is not implemented yet");
}
```

The binlog type is part of the public decode dispatch table, but the implementation is only a stub. If a `dyn_config` entry is ever written into replication binlog, slave catch-up and binlog analysis will fail at decode or apply time with `UnsupportedOperationException`.

This is latent today because there does not appear to be an active encode path creating `XDynConfig`, but the type registration makes the failure mode easy to trigger by future work.

---

## Review Feedback: Bug 1 Fix (uncommitted)

Date: 2026-04-20
Scope: uncommitted changes in `src/main/java/io/velo/repl/Repl.java` and `src/main/java/io/velo/repl/ReplRequest.java`.

### Summary of the fix

- `Repl.java`: introduced `MAX_CONTENT_LENGTH = 64 * 1024 * 1024` and a `validateContentLength()` helper, invoked on both decode (`Repl.decode()`) and encode (`Repl.buffer()`, `BufferReply.buffer()`).
- `ReplRequest.java`: pre-allocates a single `byte[expectLength]` buffer in the constructor and tracks a separate `dataLength` write-offset. `nextRead()` now writes in place via `nettyBuf.readBytes(data, dataLength, n)` instead of reallocating the accumulated byte array each chunk. Constructor also rejects `expectLength <= 0`.

### Strengths

1. **Eliminates the O(n²) copy.** `nextRead()` becomes a single in-place `readBytes` call; total copy cost across all chunks is now O(n). This directly addresses the `ReplRequest.java:124-127` concern raised in Bug 1.
2. **Bounds per-connection memory.** `ReplDecoder` asserts at most one `toFullyReadRequest` per decoder, so with the new cap a single REPL connection can pin at most ~64MB.
3. **Symmetric validation.** Length is validated on encode (`buffer()` and `BufferReply.buffer()`) as well as decode, so a local producer bug is caught at the source rather than on the peer.
4. **Defense in depth in `ReplRequest`.** The constructor now rejects `expectLength <= 0`, protecting callers that construct `ReplRequest` outside of `Repl.decode()`.

### Concerns

1. **64MB cap vs. `ToSlaveExistsBigString` payloads.** `ToSlaveExistsBigString` batches up to `ONCE_SEND_BIG_STRING_COUNT = 10` big strings per reply with no per-string size limit (`ToSlaveExistsBigString.java:27,136-148`). A single big string larger than ~64MB, or an aggregate batch exceeding 64MB, will cause `Repl.buffer()` / `BufferReply.buffer()` to throw `IllegalArgumentException` at encode time — silently breaking replication catch-up for that bucket. Chunk segments are safe: `MAX_ONCE_READ_SEGMENT_COUNT_WHEN_REPL = 256` × max segment length 65536 = 16MB, well under the cap. Recommended: either split big strings into their own frames, apply a size-aware batching policy, or raise `MAX_CONTENT_LENGTH` to cover the maximum configured big-string size.

2. **Attack surface shifted, not removed.** Any header that passes keyword/slot/type checks now allocates the full `expectLength` up front. A misbehaving or compromised peer can instantly force up to 64MB per connection. This is acceptable given REPL peers are semi-trusted (master↔slave), but the trade-off should be documented.

3. **Semantic change in `getData()` for partial reads.** `getData()` now returns `Arrays.copyOf(data, dataLength)` when not fully read, instead of the correctly-sized `data` array the old code happened to produce. `ReplDecoder.tryDecode()` only adds fully-read requests to the pipeline, so no production caller observes the partial path — but any test that inspects `getData()` on a partial request will now receive a fresh copy instead of the live buffer. Worth grepping `BinlogContentTest`, `ReplRequestTest`, etc.

4. **`copyShadow()` path is correct but subtle.** `copyShadow()` calls `getData()`; when fully read, `getData()` returns `data` directly (no copy), and the new constructor's `data.length == expectLength` fast path also skips the allocation. A one-line comment on the fully-read fast path in `getData()` would make this easier to maintain.

5. **Missing tests.** The diff does not add coverage for: (a) `Repl.decode()` rejecting `dataLength > MAX_CONTENT_LENGTH`, (b) `Repl.buffer()` / `BufferReply.buffer()` rejecting oversized content on encode, (c) `ReplRequest` constructor rejecting `expectLength <= 0`, (d) `nextRead()` correctness across multiple partial chunks with the new in-place write path.

### Verdict

The fix is structurally correct and resolves the two core issues identified in Bug 1 (unbounded allocation and O(n²) copy). Before commit, recommended follow-ups:

- Verify the largest legitimate big-string payload fits within `MAX_CONTENT_LENGTH`, or adjust the batching / cap accordingly.
- Add decoder + encoder tests for the length cap.
- Audit tests that exercise partial-read `getData()` for the new copy semantics.

---

## Round 4 Author Findings (AI agent 1)

Date: 2026-04-20

### Bug 4: slave catch-up advances replication state before async apply work has actually executed

**Severity:** High
**Files:** `src/main/java/io/velo/repl/TcpClient.java:140-168`, `src/main/java/io/velo/command/XGroup.java:1609-1666`, `src/main/java/io/velo/repl/incremental/XWalV.java:163-179`, `src/main/java/io/velo/repl/incremental/XBigStrings.java:183-196`

```java
BinaryChannelSupplier.of(ChannelSuppliers.ofSocket(socket))
        .decodeStream(new ReplDecoder())
        .mapAsync(pipeline -> {
            ...
            var reply = xGroup.handleRepl(replRequest);
```

```java
for (var content : decodedContents) {
    content.apply(slot, replPair);
}
...
replPair.setSlaveLastCatchUpBinlogFileIndexAndOffset(
        new Binlog.FileIndexAndOffset(fetchedFileIndex, fetchedOffset + readSegmentLength));
```

```java
oneSlot.asyncExecute(() -> {
    var putResult = targetWal.put(isValueShort, key, v2);
    if (putResult.needPersist()) {
        oneSlot.doPersist(walGroupIndex, key, putResult);
    }
});
```

The slave handles REPL traffic on the TCP client event-loop thread, not on each destination slot worker thread. During catch-up, `s_catch_up()` decodes one binlog segment, invokes `content.apply(...)`, and then immediately advances the slave's catch-up offset and readability state. But several `apply()` implementations, notably `XWalV` and `XBigStrings`, only enqueue the real persistence work via `oneSlot.asyncExecute(...)` and return immediately.

That means the replica can mark a segment as consumed before the writes for that segment have actually run on the target slot threads. A process crash, disconnect, or failover in that window can leave the slave believing it already applied data that was still sitting in per-slot async queues. The same ordering pattern exists in full-sync paths for short strings and existing big strings, so this is a replay consistency issue, not just an incremental catch-up issue.

### Bug 5: incremental big-string fetch can drop the file payload permanently while marking the fetch as complete

**Severity:** High
**Files:** `src/main/java/io/velo/repl/incremental/XBigStrings.java:183-196`, `src/main/java/io/velo/repl/ReplPair.java:782-835`, `src/main/java/io/velo/command/XGroup.java:946-1000`

```java
oneSlot.asyncExecute(() -> {
    oneSlot.put(key, s.bucketIndex(), cv, true);
});
replPair.addToFetchBigStringId(uuid, bucketIndex, keyHash, key);
```

```java
var bigStringBytes = bigStringFiles.getBigStringBytes(uuid, s.bucketIndex(), s.keyHash());
if (bigStringBytes == null) {
    ...;
    bigStringBytes = new byte[0];
}
```

```java
if (buffer.hasRemaining()) {
    ...
    oneSlot.asyncExecute(() -> {
        bigStringFiles.writeBigStringBytes(uuid, s.bucketIndex(), s.keyHash(), bigStringBytes);
    });
}

replPair.doneFetchBigStringUuid(uuid);
```

`XBigStrings.apply()` stores the metadata/value first and only records the actual big-string file fetch in `ReplPair`'s in-memory fetch queues. Later, when the slave requests `incremental_big_string`, the master returns an empty payload if the backing big-string file is already gone. On the slave side, `s_incremental_big_string()` treats "no remaining payload bytes" as a skip, but still calls `doneFetchBigStringUuid(uuid)` unconditionally.

As a result, a replica can end up with the logical metadata for a big string but without the underlying big-string bytes on disk, and the fetch token is removed anyway. Because the to-fetch / doing-fetch lists are transient in-memory state, reconnect or restart leaves no durable retry path for that missing file. This can surface later as a key that exists in the index but whose big-string content is unavailable or corrupted from the slave's point of view.
