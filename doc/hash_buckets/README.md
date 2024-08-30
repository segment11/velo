# velo use hash buckets to store data

## key buckets

- one sharding data use a single slot
- one slot use n (65536 or 512K) key buckets for different estimated count of keys
- one key bucket use 4K bytes to store keys, max 48 keys
- key buckets only store key bytes, key hash (long type), expire time (long) type, and value offset
- key buckets will store small value bytes directly if the value bytes size is less than 32 bytes or 64 bytes by config
- one key bucket can split to 3 key buckets if the key count is more than 48, max 2 splits, 1 -> 9 key buckets, max 9 *
  48 = 432 keys, when key bucket split, it will rehash all keys to new key buckets, performance is not good
- so one slot can store max 512K * 48 * 9 = 216M keys
- when one key bucket find one key, it will iterate all keys to find the key, compare the key hash and key bytes, if
  expired, return not found

## chunk segments

- chunk segments store value bytes
- one slot use a chunk, one chunk use more than one file to store value bytes, file number can be 1 / 2 / 4 / 8 / 16 /
  64, depends on the value bytes size and compress ratio
- one file max size is 2G, so one chunk max size is 128G, usually can store one slot max 216M keys
- one chunk file has many segments, one segment length can be 4K / 16K / 64K / 256K, depends on the value bytes size and
  compress ratio
- one segment is compressed by zstd, use no trained dictionary, and use default zstd compression level 3
- one segment has 1-4 sub-segments, one sub-segment has a batch of value bytes, and value bytes are compressed by zstd
  using a trained dictionary

## wal groups

- 16 or 32 key buckets are in one wal group logically
- one slot has max 512K / 16 = 32K wal groups, all wal groups are in one wal file
- one slot has 2 wal files, one is for short values (a value length is less than 32 bytes or 64 bytes), one is for long
  values
- when delete one key, it will write a delete record to wal file, use short value wal file
- when one wal group is full, ether by key count or key + value bytes size, it will cause a flush to chunk segments
- after chunk segments flush, the key buckets will batch update the value offset in chunk segments
- when short value wal group is full, only update the key buckets, need not flush chunk segments
- after key buckets update, the slot will reset the wal group, and continue to write new records
- all value bytes saved in wal groups temporarily ares already compressed by zstd using a trained dictionary
- when restart the server, it will read all wal files to recover the slot data
- when chunk segments flush, begin with a new file if one chunk file available segments count is less then need write
  segments count

## merge chunk segments

- when all chunk segments files are half full, it will begin to merge chunk segments from the first file
- when new chunk segments are written, it will merge chunk segments relative to the first file, always keep 1/3 or half
  chunk segments are available to write new value bytes
- once a merge job only merge chunk segments those are in the same wal group, so when compare to exists keys stored
  in key buckets it will only need read 16 or 32 key buckets, for performance
- before write chunk segments when one wal group is full, it will pre-read 1-2 chunk segments with the same wal group
  need to merge, and compare the keys with key buckets together, so actually, the merge job is not so heavy, this is
  designed for performance / low latency
- when a merge job is done, valid key / values are temporarily saved in memory, and then write to new chunk segments
  when key count are too many, or merged segments count are too many, after write new chunk segments, it will update the
  meta flag of merged chunk segments and removed temporary saved key / values
- write chunk segments are always group by wal group, for performance

## meta data

### key buckets: by bucket index, split number + key count

### chunk segments: by segment index, available flag + lsn + wal group index

- one chunk segment available flag can be init / new_write / reuse_new / merging / merged / merged_and_persisted / reuse
- only init / merged_and_persisted / reuse can be reused to write new value bytes