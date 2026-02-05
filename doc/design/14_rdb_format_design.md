# Velo RDB Format Design

## Overview

Velo supports importing/exporting Redis RDB format for data migration and backup.

## RDB Version Support

- **RDB Version:** 11
- **Min Version:** 6

## Supported Type Codes

| Type Code | Type | Description |
|-----------|------|-------------|
| 0 | RDB_TYPE_STRING | String |
| 1 | RDB_TYPE_LIST | List |
| 2 | RDB_TYPE_SET | Set |
| 3 | RDB_TYPE_ZSET | Sorted Set |
| 4 | RDB_TYPE_HASH | Hash |
| 9 | RDB_TYPE_HASH_ZIPMAP | Compressed hash |
| 10 | RDB_TYPE_LIST_ZIPLIST | ZipList encoded list |
| 11 | RDB_TYPE_SET_INTSET | Integer set |
| 12 | RDB_TYPE_ZSET_ZIPLIST | ZipList encoded ZSet |
| 13 | RDB_TYPE_HASH_ZIPLIST | ZipList encoded hash |
| 14 | RDB_TYPE_LIST_QUICKLIST | QuickList encoded list |
| 18 | RDB_TYPE_LIST_QUICKLIST2 | QuickList encoded list v2 |
| 16 | RDB_TYPE_HASH_LISTPACK | ListPack encoded hash |
| 17 | RDB_TYPE_ZSET_LISTPACK | ListPack encoded ZSet |

## String Encodings

```
Byte Length Encoding:
  6-bit:   [0xxxxxxx]                    → 0-63 bytes
  14-bit:  [10xxxxxx](x)                 → 64-16383 bytes
  32-bit:  [11xxxxxx][xxxxxxxx][...][x] → ≥ 16384 bytes

Integer Encoding:
  8-bit:  110xxxxx                       → Int8
  16-bit: 1110xxxx [xxxxxxxx]            → Int16
  32-bit: 11110xxx [xxxxxxxx][...]       → Int32
  LZF:   [11111xxx][...] + [length] + [data] → Compressed integer
```

## Encoding/Decoding

### RDBParser

```java
public class RDBParser {
    public static void parse(byte[] rdbData, RDBCallback callback) {
        ByteArrayInputStream bais = new ByteArrayInputStream(rdbData);
        DataInputStream dis = new DataInputStream(bais);

        // 1. Read RDB header
        byte[] magic = new byte[9];
        dis.readFully(magic);
        // "REDIS" + version

        // 2. Read length
        byte[] lenBytes = new byte[4];
        dis.readFully(lenBytes);
        int rdbLength = Utils.readInt(lenBytes, 0);

        // 3. Switch to CRC input stream
        CRCInputStream crcIn = new CRCInputStream(bais);

        // 4. Read key-value pairs
        while (true) {
            intopcode = crcIn.readByte();

            if (opcode == RDB_OPCODE_EOF) {
                break;  // End of file
            }

            if (opcode == RDB_OPCODE_AUX) {
                // Aux field (e.g., "checksum", "expires")
                String key = readString(crcIn);
                byte[] value = readString(crcIn);
                continue;
            }

            if (opcode == RDB_OPCODE_SELECTDB) {
                // Database selection
                int db = crcIn.readByte();
                continue;
            }

            if (opcode == RDB_OPCODE_RESIZEDB) {
                // Resize database
                int dbSize = readLength(crcIn);
                continue;
            }

            // Read key
            byte[] key = readString(crcIn);

            // Read value type
            typeCode = crcIn.readByte();

            // Read and decode value
            switch (typeCode) {
                case RDB_TYPE_STRING:
                    String str = readString(crcIn);
                    callback.onString(key, str);
                    break;

                case RDB_TYPE_LIST:
                    RedisList list = new RedisList();
                    readList(crcIn, list);
                    callback.onList(key, list);
                    break;

                case RDB_TYPE_HASH:
                    RedisHH hash = new RedisHH();
                    readHash(crcIn, hash);
                    callback.onHash(key, hash);
                    break;

                // ... other types
            }
        }

        // 5. Verify CRC
        long expectedCrc = crcIn.getCrcValue();
        long crc64 = Utils.readLong(bais);
        if (expectedCrc != crc64) {
            throw new IOException("CRC mismatch");
        }
    }
}
```

### Value Decoding

```
ZipList Decoding (RDB_TYPE_LIST_ZIPLIST):
┌────────────────────────────────────┐
│ zlbytes (2B)                        │ ← Total bytes
│ zltail (2B)                         │ ← Offset of tail
│ zllen (2B)                           │ ← Number of entries
│ entries (var)                         │
│   - [len] [data]                     │
│   - [len] [data]                     │
│   - ...                              │
│ pend (var)                           │ ← Unused space
└────────────────────────────────────┘

ListPack Decoding (RDB_TYPE_HASH_LISTPACK):
┌────────────────────────────────────┐
│ total-bytes (var)                   │
│ num-elements (var)                   │
│ element-encoding-start-byte         │
│ encoding-type (var)                  │
│ element-1 (var)                     │
│   [len] [key]   [len] [value]     │
│ element-2 (var)                     │
│ element-end-marker                  │
└────────────────────────────────────┘
```

## Dump Commands

```
DUMP key
  → Returns RDB-encoded value bytes

RESTORE key value
  → Import RDB-encoded value
```

## RDB Importer

```java
public class RDBImporter {
    public static void importFromFile(String path) {
        try {
            byte[] rdbData = Files.readAllBytes(Paths.get(path));

            RDBParser.parse(rdbData, new RDBCallback() {
                @Override
                public void onString(byte[] key, String value) {
                    // SET key value
                    execute("SET", key, value.getBytes());
                }

                @Override
                public void onHash(byte[] key, RedisHH hash) {
                    // HMSET key field1 value1 field2 value2 ...
                    // Or use single key storage if configured
                    for (Map.Entry<String, byte[]> entry : hash.map.entrySet()) {
                        String fullKey = new String(key) + ":" + entry.getKey();
                        execute("SET", fullKey.getBytes(), entry.getValue());
                    }
                }

                @Override
                public void onList(byte[] key, RedisList list) {
                    for (byte[] element : list.getElements()) {
                        execute("RPUSH", key, element);
                    }
                }

                // ... other types
            });

        } catch (Exception e) {
            log.error("Failed to import RDB file", e);
        }
    }
}
```

## Example RDB Format

```
REDIS0011                     (Magic + version)
0                             (DB selector)
5                             (Key size: "mykey")
mykey                         (Key bytes)
0                             (Type: String)
5                             (Value size)
world                         (Value)
255                           (EOF marker)
[64-bit CRC]                  (Checksum)
```

## Compatibility

Redis兼容性:

- 支持Redis RDB格式导入/导出
- 与Redis客户端兼容(dump/restore/迁移)
- 支持压缩类型(Ziplist, ListPack)

## Related Documentation

- [Type System Design](./03_type_system_design.md) - Redis types

## Key Source Files

- `src/main/java/io/velo/rdb/RDBParser.java` - RDB parsing
- `src/main/java/io/velo/rdb/RDBImporter.java` - Import functionality

---

**Version:** 1.0  
**Last Updated:** 2025-02-05  
