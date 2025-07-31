# Velo use a compress value object to store value bytes

## compress value

A compress value header includes:

- lsn (long)
- expire time (long)
- dictionary id (int)
- key hash (long)
- compressed size (int)

TIPS:
Velo support max key length is 256 bytes, compressed value length is 64K.

## value type

- number byte
- number short
- number int
- number long
- deleted flag record
- short string
- big string
- hash fields + values in split
- hash fields + values together
- list
- set
- zset
- geo
- hyperloglog
- bloom filter
- normal string

TIPS: a deleted record is short string type

## big string

A big string value is stored in a single file in a specific directory, the file name is a unique lsn, and the file
content is compressed by zstd using a share trained dictionary or self trained dictionary.