# Persist Rebuild From Chunk Admin Command

## Goal

Add a manual admin recovery command for Bug 2 in the persist write flow: when chunk segment bytes and `HAS_DATA` flags
exist but key-bucket update failed, rebuild one WAL group's key buckets from chunk data.

## Decision

Use `preview` and `apply` modes:

- `preview` is read-only. It scans chunk `HAS_DATA` segments for one WAL group, decodes records, resolves live records in
  memory, and returns stats.
- `apply` runs the same scan and then replaces that WAL group's key-bucket index with the resolved live records.

The command runs on the slot worker thread. Velo's run-to-completion slot-thread model makes the rebuild thread-safe for
the target slot because other client commands for that slot queue until recovery returns. The tradeoff is slot latency:
the target slot is blocked while the command scans and rebuilds.

## Command

```text
manage slot <slot> rebuild-from-chunk <walGroupIndex> preview
manage slot <slot> rebuild-from-chunk <walGroupIndex> apply
```

## Data Flow

```text
meta chunk segment flags
 -> filter HAS_DATA segments for the target WAL group
 -> read each chunk segment
 -> decode compressed values
 -> verify each key maps to the target WAL group
 -> keep latest seq per key/keyHash
 -> filter expired records
 -> preview: return stats only
 -> apply: replace key buckets for this WAL group
```

## Safety Rules

- Do not reset `HAS_DATA` flags when key-bucket update fails; chunk data is the recoverable source.
- Keep the command scoped to one WAL group to bound memory.
- `preview` must not mutate key buckets, segment flags, WAL, LRU, or metadata.
- `apply` must rebuild from decoded chunk records, not merge with the potentially broken current key-bucket index.
- WAL replay remains a separate later recovery step for full data-broken recovery.

## Stats

The command returns JSON with:

- `slot`
- `walGroupIndex`
- `mode`
- `segmentsScanned`
- `segmentsInvalid`
- `recordsDecoded`
- `recordsExpired`
- `recordsWalGroupMismatch`
- `recordsAfterSeqMerge`
- `staleRecords`
- `bucketsTouched`
- `applied`
