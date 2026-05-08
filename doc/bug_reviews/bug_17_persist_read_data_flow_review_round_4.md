# Bug 17 Persist Layer Read Data Flow Review Round 4

## Scope

Static review of the persist layer **read** data flow. This round focuses on the complete read path from `OneSlot.get()`, `OneSlot.exists()`, `OneSlot.getExpireAt()` through WAL, slot KV LRU, keyLoader, and key buckets.

Reviewed paths:

- `OneSlot.get()` — full read path: WAL → LRU → keyLoader (short value + PVM/segment)
- `OneSlot.getExpireAt()` — TTL read path: WAL → LRU → keyLoader
- `OneSlot.exists()` — existence check: WAL → keyLoader
- `OneSlot.getFromWal()` — WAL read helper
- `Wal.getV()` — WAL value retrieval with seq comparison
- `KeyLoader.getValueXByKey()` / `KeyLoader.getExpireAtAndSeqByKey()` — key bucket reads
- `KeyBucket.getValueXByKey()` / `KeyBucket.getExpireAtAndSeqByKey()` — cell-level lookup
- `CompressedValue.decode()` — CV deserialization (normal + number/short string formats)
- `BaseCommand.getCv()` — the single caller of `OneSlot.get()`
- `BigStringFiles.getBigStringBytes()` — big-string file read with LRU cache
- `FdReadWrite.readInnerByBuffer()` — file I/O with Zstd-compressed LRU cache
- `Chunk.readOneSegment()` — segment read for PVM values

## Finding 1: `get()` LRU path does not check expiry — stale expired data returned to caller

**Severity:** N/A — Not a bug, by design

The contract is that `get()` returns raw data and callers check expiry. This is the same pattern as `getValueXByKey` which also doesn't check expiry — the caller `get()` itself checks at line 1572. The sole caller of `get()` (`BaseCommand.getCv()`) checks `cv.isExpired()` at line 634 immediately after decoding. The LRU path is consistent with this layered design.

## Finding 2: `exists()` and `get()` `isDeleted` check — `true` branch unreachable for current delete path

**Severity:** N/A — Not a bug, defensive code

JaCoCo confirms the `isDeleted` check at line 1706 IS executed (`pc bpc` — 1 of 2 branches missed). The check runs for every normal WAL entry (where `isDeleted` returns false). The `true` branch is never hit because `getFromWal()` already filters out delete tombstones via `isExpired()` (since `EXPIRE_NOW = -1` is always "expired"). The check is defensive — if a future code path creates a delete entry with `expireAt = NO_EXPIRE`, it would be caught here. Not dead code, just redundant for the current delete path.

## Finding 3: `KeyBucket.getExpireAtAndSeqByKey()` returns null for first expired match, skipping later non-expired matches

**Severity:** N/A — Not a bug, unreachable scenario

A key can never exist in two cells in the same KeyBucket:
- `put()` always calls `del()` first (line 584), removing any existing instance before inserting
- `rePutAll()` iterates existing (unique) entries and re-inserts them sequentially — no duplicates introduced
- The merge path (`putPvmListToTargetBucket`) uses a `HashMap<String, PersistValueMeta>` which deduplicates by key
- `del()` breaks after the first match (line 966), but since keys are always unique, this is correct

The early return on expired match in `getExpireAtAndSeqByKey()` is safe because the first match is always the only match.

## Summary

| Finding | Severity | Status | Confidence |
|---------|----------|--------|------------|
| 1 — `get()` LRU path skips expiry check | ~~Low~~ N/A | **NOT A BUG** | By design — callers check expiry, same as `getValueXByKey` |
| 2 — `isDeleted` check in `get()`/`exists()` | ~~Low~~ N/A | **NOT A BUG** | Dead code for `true` branch — `EXPIRE_NOW` always makes `isExpired()` catch delete first; `false` branch IS executed (JaCoCo confirms) for every normal WAL entry |
| 3 — `getExpireAtAndSeqByKey` stops on first expired match | ~~Low~~ N/A | **NOT A BUG** | Keys are always unique in a bucket — `put()` deletes before inserting, merge uses HashMap |

## Reviewer Verification (AI Agent 2) — Round 5 Confirmation

### Finding 1: `get()` LRU path skips expiry check

**NOT A BUG — By design**

Verified: the contract is that `get()` returns raw data and callers check expiry. The sole caller `BaseCommand.getCv()` checks `cv.isExpired()` at line 634. The asymmetry with `getExpireAt()` is because `getExpireAt()` has no external caller to perform the check.

### Finding 2: `isDeleted` check in `exists()`

**NOT A BUG — Dead code for `true` branch, `false` branch IS executed**

Verified via test trace and JaCoCo:
- `isDeleted()` line (1706) IS executed for every normal WAL entry — JaCoCo confirms `pc bpc` (partial branch coverage: `false` branch hit, `true` branch not hit)
- `true` branch never reached because all delete tombstones use `EXPIRE_NOW = -1` → `getFromWal()`'s `isExpired()` catches them first (line 1675-1678)
- Trace: `removeDelay()` → `EXPIRE_NOW = -1` → `getFromWal()` returns `null` with `isExpiredFlagArray[0] = true` → line 1709 returns `false` → line 1706 never reached
- `false` branch IS reached after every normal `put()` — `getFromWal()` returns CV bytes → `isDeleted()` returns `false` → `exists()` returns `true`

The check is technically dead code for the `true` branch (current delete path), but the `false` branch is exercised by every normal read. The `true` branch would only be reached if a future code path created a delete entry with `expireAt = NO_EXPIRE` instead of `EXPIRE_NOW`.

### Finding 3: `getExpireAtAndSeqByKey` stops on first expired match

**NOT A BUG — Unreachable scenario**

Verified all three deduplication paths:
1. `put()` calls `del()` first (KeyBucket.java:584)
2. `rePutAll()` preserves uniqueness — iterates existing unique entries and re-inserts (KeyBucket.java:623-645)
3. Merge path uses `HashMap<String, PersistValueMeta>` which deduplicates by key (KeyBucketsInOneWalGroup.java:247)

The first match is always the only match — early return on expired is safe.

## Overall Assessment

The persist layer read data flow is well-structured. The main read path (`get()` → WAL → LRU → keyLoader → segment) correctly handles all value types. WAL takes precedence over keyLoader. LRU is invalidated on persist and flush. Expiry checks are present at every level.

**All 3 findings are NOT bugs — no fixes needed.**
