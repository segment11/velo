# Big String UUID Design

**Problem**

The big-string persistence path currently derives the file `uuid` from `keyHash`. That means two distinct keys that collide on the 64-bit hash can alias the same physical big-string file and the same in-memory cache entry.

**Decision**

Keep the file path shape as:

`bucketIndex/<uuid>_<keyHash>`

But change the meaning of `uuid`:

- `uuid` becomes a true unique identifier for the big-string payload
- `keyHash` remains a secondary field used for file naming, lookup validation, and existing cleanup/repl code

**Write Path**

- In `BaseCommand.set(...)`, generate a fresh big-string `uuid` from the existing sequence source instead of reusing `slotWithKeyHash.keyHash`.
- In `OneSlot.put(...)`, generate a fresh big-string `uuid` instead of reusing `cv.getKeyHash()`.
- Store that real `uuid` in `CompressedValue.setCompressedDataAsBigString(uuid, ...)`.
- Write the file with `BigStringFiles.writeBigStringBytes(uuid, bucketIndex, keyHash, ...)`.

**Read / Cleanup / Replication**

- No file-path migration is needed because readers already use both `uuid` and `keyHash`.
- `BigStringFiles`, `KeyLoader`, delayed delete, and repl fetch can keep their existing path shape and APIs.
- The main semantic change is that `uuid != keyHash` for newly written big strings.

**Compatibility**

- Existing files with `uuid == keyHash` remain readable because the format is unchanged.
- New files use a different `uuid`, but the encoded metadata already stores the `uuid` explicitly.

**Testing**

- Add a focused regression that proves two big strings with the same forced `keyHash` produce different `uuid`s and separate files.
- Keep an existing cleanup test green to prove delayed delete and persisted big-string scanning still work with non-hash UUIDs.
