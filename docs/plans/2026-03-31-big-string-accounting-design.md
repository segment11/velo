# Big String Accounting Design

**Problem**

`BigStringFiles` updates file-count, disk-usage, and byte metrics from intent rather than from actual filesystem effect:

- sliced writes use `bytes.length` instead of the `length` argument
- delete accounting is updated before `file.delete()` is known to have succeeded

This makes `big_string_files_disk_usage`, write-byte stats, and delete-related counters drift away from reality.

**Decision**

Keep storage behavior unchanged and fix only accounting semantics:

- write accounting must use the actual slice length written
- delete accounting must update only after successful deletion

**Write Path**

- In `writeBigStringBytes(..., offset, length)`, use `length` instead of `bytes.length` when updating:
  - `diskUsage`
  - `writeByteLengthTotal`

**Delete Path**

- In `deleteBigStringFileIfExist(...)`, call `file.delete()` first.
- If it succeeds, then update:
  - `bigStringFilesCount`
  - `diskUsage`
  - delete byte/file counters
- If it fails, leave accounting unchanged and return `false`.

**Testing**

- Add a regression proving a sliced write updates accounting from the slice length.
- Add a regression proving failed delete leaves count and disk-usage unchanged.
