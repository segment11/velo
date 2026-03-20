# Velo Replication Design

## Table of Contents
- [Overview](#overview)
- [Replication Architecture](#replication-architecture)
- [Master-Slave Model](#master-slave-model)
- [Binlog Format](#binlog-format)
- [Replication Phases](#replication-phases)
- [Replication Protocol](#replication-protocol)
- [Failover](#failover)
- [Configuration Compatibility](#configuration-compatibility)
- [Related Documentation](#related-documentation)

---

## Overview

Velo implements **pull-based master-slave replication** with automatic failover. Replication supports:
- **Pre-catchup phase**: Slave fetches existing data from master
- **Incremental sync**: Real-time binlog streaming
- **Auto failover**: Zookeeper/Redis Sentinel coordination
- **Configuration validation**: Ensures master-slave compatibility

### Replication Modes

| Mode | Description | Use Case |
|------|-------------|----------|
| Master-Only | Single instance | Development, no HA |
| Master-Slave | One leader, one follower | High availability |
| Chain | Master → Slave → Slave | Multi-layer HA |
| Cluster | Multiple slots, multiple replicas | Large scale |

---

## Replication Architecture

### Component Diagram

```
┌─────────────────────────────────────────────────────────────┐
│                      Master Instance                          │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  OneSlot[0]   ...   OneSlot[N-1]                     │  │
│  │    │                          │                       │  │
│  │    ▼                          ▼                       │  │
│  │  Binlog[0]              Binlog[N-1]                     │  │
│  │  - XWalV records         - XWalV records               │  │
│  │  - Segmented (16KB)      - Segmented                   │  │
│  │  - ROTATING              - ROTATING                      │  │
│  └──────────────────────────────────────────────────────┘  │
│                         │                                   │
│                    [Networking]                            │
│      MasterSlave  ────────┼───────────── SlaveMaster        │
│      Server      │                                   │
│                   ▼                                   ▼
│  ┌────────────────────────────────────────────────────┐ │
│  │  Slave Instance                                 │ │
│  │  ┌────────────────────────────────────────────┐  │ │
│  │  │ ReplPair[0]                            │  │ │
│  │  │   - state: EXISTS_WAL → CATCH_UP         │  │ │
│  │  │   - lastOffset: tracking binlog          │  │ │
│  │  │   - socket: connection to master         │  │ │
│  │  ├────────────────────────────────────────────┤  │ │
│  │  │ OneSlot[0]                             │  │ │
│  │  │   - Replaying binlog entries              │  │ │
│  │  │   - Updating key buckets & chunks         │  │ │
│  │  └────────────────────────────────────────────┘  │ │
│  └────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘

┌─────────────────────────────────────────────────────────────┐
│                      Coordination Layer                       │
│  ┌──────────────────────────────────────────────────────┐  │
│  │  LeaderSelector (ZooKeeper/Curator)                 │  │
│  │  - Leader latch election                           │  │
│  │  - Master detection                                │  │
│  │  - Failover coordination                           │  │
│  └──────────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────────┘
```

### Flow Diagram

```mermaid
sequenceDiagram
    participant S as Slave
    participant M as Master
    participant B as Binlog
    participant F as Failover

    S->>M: HELLO (slave_uuid)
    M->>S: HI (master_uuid)

    Note over S,M: Phase 1: Pre-Catchup

    S->>M: exists_wal
    M->>B: Read WAL entries
    B-->>M: WAL data
    M-->>S: s_exists_wal (WAL entries)

    S->>M: exists_chunk_segments
    M-->>S: s_exists_chunk_segments

    S->>M: exists_dict
    M-->>S: s_exists_dict

    S->>M: exists_all_done
    M-->>S: ACK

    Note over S,M: Phase 2: Incremental Replication

    loop Every 50ms
        S->>M: catch_up (offset)
        M->>B: Read binlog at offset
        B-->>M: Binlog segment
        M-->>S: s_catch_up (binlog bytes)
        S->>B: Decode and apply entries
    end

    Note over S,M,F: Phase 3: Failover (if needed)

    Master failure
    F->>M: Lost (heartbeat timeout)
    F->>F: Leader election
    F->>S: You are now candidate
    S->>S: isAllCaughtUp?
    S->>F: Promote to master
    F->>F: Update leader
```

---

## Master-Slave Model

### Pull-Based Architecture

```
Master                                       Slave
  │                                           │
  │ Write                                    │
  ▼                                           │
┌─────────┐     persist                     │
│   WAL   │ ──────────────────┐            │
│  HashMap│  (group flush)    │            │
└────┬────┘          │        │            │
     │               ▼        │            │
     │           ┌─────────┐   │            │
     │           │  Binlog │   │            │
     │           │  Append │◄──┼────────────┤
     │           └─────────┘   │  catch_up  │
     │                │        │  request    │
     │                ▼        │            │
┌─────────┐         segment   │            │
│  Chunk  │ ◀───────── bytes ───┤            │
│ Segments│                   │  ┌─────────────┐
└─────────┘                   │  │  OneSlot  │
                                │  │  Apply     │
                                │  │  entries   │
                                │  └─────────────┘
                                ▼
                            ┌───────────┐
                            │  Replica  │
                            │  Keys     │
                            └───────────┘
```

### Replication Features

**Per-Slot Binlog:**
- Each slot (0 to N-1) has independent binlog
- Binlog contains write operations (XWalV entries)
- Segmented for efficient random access

**Pull-Based:**
- Slave controls replay speed
- Master streams segments on request
- No blocking of master for slow slaves

**Pre-Catchup:**
- Slave fetches existing data before incremental sync
- Reduces catch-up time
- Ensures data consistency

**Binlog Retention:**
- Configurable retention period
- Automatic cleanup of old segments
- Space management with rotation

---

## Binlog Format

### Binlog File Structure

```
binlog-{slot}/
├── binlog-0   (File 0)
├── binlog-1   (File 1)
├── binlog-2   (File 2)
├── ...
└── binlog-N   (File N)

Each file: ≤ binlogOneFileMaxLength (default: 32MB)
Segment size: binlogOneSegmentLength (default: 262,144 bytes = 256KB)
Segments per file: binlogOneFileMaxLength / binlogOneSegmentLength = 128
```

### File and Segment Relationship

Each binlog file contains multiple fixed-size segments:

```
File binlog-0 (32MB):
  ┌────────────────────────────────────────────────────────┐
  │ Segment 0     │ Segment 1     │ ... │ Segment 127      │
  │ (256KB)       │ (256KB)       │     │ (256KB)          │
  └────────────────────────────────────────────────────────┘
  Offset: 0         262144         ...   33292288

File binlog-1 (32MB):
  ┌────────────────────────────────────────────────────────┐
  │ Segment 128   │ Segment 129   │ ... │ Segment 255      │
  │ (256KB)       │ (256KB)       │     │ (256KB)          │
  └────────────────────────────────────────────────────────┘

Segment is the unit for replication catch-up, not the file.
Master reads one segment at a time and sends to slave.
```

### Binlog Entry Types

```java
public enum BinlogEntryType {
    WAL_V(1, "XWalV - Write Ahead Log record"),
    BIG_STRINGS(10, "XBigStrings - Large value replication"),
    ACL_UPDATE(20, "XAclUpdate - ACL change"),
    DICT(100, "XDict - Dictionary update"),
    SKIP_APPLY(110, "XSkipApply - Skip operation"),
    UPDATE_SEQ(120, "XUpdateSeq - Sequence update"),
    DYN_CONFIG(121, "XDynConfig - Configuration change"),
    FLUSH(-128, "XFlush - Flush to persist");

    private final int code;
    private final String description;
}
```

### XWalV Entry Format

```
Type code: 1 (XWalV)

Format:
┌──────────────────────────────────────────────────────────┐
│ type (byte)                                              │
│ encodedLength (int 4B)                                   │
│ isValueShort (byte 1B)                                   │
│ seq (long 8B)                                           │
│ bucketIndex (int 4B)                                     │
│ keyHash (long 8B)                                        │
│ expireAt (long 8B)                                       │
│ spType (int 4B)   - dict seq or special type           │
│ keyLength (short 2B)                                      │
│ keyBytes (variable)                                      │
│ cvEncodedLength (int 4B)                                 │
│ cvEncoded (variable)                                     │
└──────────────────────────────────────────────────────────┘

Example: SET key "value"
  type: 1
  encodedLength: 62 (total bytes after this field)
  isValueShort: 1
  seq: 123456789
  bucketIndex: 1024
  keyHash: 9876543210123456789
  expireAt: 0 (no expiry)
  spType: 15 (trained dict seq)
  keyLength: 3
  keyBytes: "key"
  cvEncodedLength: 25
  cvEncoded: [25 compressed bytes]
```

### Binlog Offset Calculation

```
Global offset = (fileIndex * binlogOneFileMaxLength) + fileOffset

Example:
  File index: 0
  File offset: 524288
  BinlogOneFileMaxLength: 33554432 (32MB)

  Global offset = 0 * 33554432 + 524288 = 524288

Navigation:
  File index = offset / binlogOneFileMaxLength
  File offset = offset % binlogOneFileMaxLength
```

### Binlog Append Flow

```java
public class Binlog {
    private static final int SEGMENT_SIZE = 262144;

    public void append(XWalV v) {
        // 1. Encode V record
        byte[] encoded = v.encode();

        // 2. Write to latest segment
        int currentFile = getCurrentFileIndex();
        long currentOffset = getCurrentFileOffset();

        // 3. Check segment boundary
        if (currentOffset + encoded.length > SEGMENT_SIZE) {
            // Move to next segment
            advanceSegment();
            currentFile = getCurrentFileIndex();
            currentOffset = 0;
        }

        // 4. Write to file
        writeFile(currentFile, currentOffset, encoded);
        incrementOffset(encoded.length);

        // 5. Track offset for this V
        v.setBinlogOffset(calculateGlobalOffset(currentFile, currentOffset));
    }
}
```

---

## Replication Phases

### Phase 1: Handshake

**Hello Message (Slave → Master):**
```
ReplType: hello (0)
Content:
┌────────────────────────────────────────────────────┐
│ slaveUuid (long 8B)                               │
│ listenAddressesLength (int 4B)                    │
│ listenAddresses (var string)                      │
│ replProperties (ReplProperties record)             │
│   - bucketsPerSlot                                │
│   - oneChargeBucketNumber                         │
│   - segmentNumberPerFd                            │
│   - fdPerChunk                                    │
│   - segmentLength                                 │
│   - isSegmentUseCompression                       │
└────────────────────────────────────────────────────┘
```

**Hi Message (Master → Slave):**
```
ReplType: hi (1)
Content:
┌────────────────────────────────────────────────────┐
│ masterUuid (long 8B)                              │
│ replPropertiesMatch (boolean 1B)                    │
└────────────────────────────────────────────────────┘
```

### Phase 2: Pre-Catchup

**Fetch Existing Data:**

```
Slave Request:
  REplType: exists_wal (100)
  Content: slot (2B)
Master Response:
  REplType: s_exists_wal (200)
  Content: [Batch of WAL V records]

Slave Request:
  REplType: exists_chunk_segments (101)
  Content: slot, segmentCount, segmentIndices[]
Master Response:
  REplType: s_exists_chunk_segments (201)
  Content: [Batch of segment data]

Slave Request:
  REplType: exists_dict (103)
  Content: slot, dictSeq
Master Response:
  REplType: s_exists_dict (203)
  Content: Dict bytes

Slave Finalize:
  REplType: exists_all_done (199)
  Content: slot
Master Response:
  ACK
```

### Phase 3: Incremental Sync

**Catch-Up Message Format (Slave → Master):**

```
ReplType: catch_up (27)
Content:
  ┌──────────────────────────────────────────────────────┐
  │ binlogMasterUuid (long 8B)                           │
  │ needFetchFileIndex (int 4B)                          │
  │ needFetchOffset (long 8B) - aligned to segment       │
  │ lastUpdatedOffset (long 8B) - exact position         │
  └──────────────────────────────────────────────────────┘
```

**Catch-Up Response Format (Master → Slave):**

```
ReplType: s_catch_up (37)
Content (with segment data):
  ┌──────────────────────────────────────────────────────┐
  │ isMasterReadonly (byte 1B)                           │
  │ needFetchFileIndex (int 4B)                          │
  │ needFetchOffset (long 8B)                            │
  │ masterCurrentFileIndex (int 4B)                      │
  │ masterCurrentOffset (long 8B)                        │
  │ readSegmentLength (int 4B)                           │
  │ readSegmentBytes (var)                               │
  └──────────────────────────────────────────────────────┘

Content (all caught up, no segment data):
  ┌──────────────────────────────────────────────────────┐
  │ isMasterReadonly (byte 1B)                           │
  │ masterCurrentFileIndex (int 4B)                      │
  │ masterCurrentOffset (long 8B)                        │
  └──────────────────────────────────────────────────────┘
```

**Pull Loop with Offset Alignment:**

```
while (!isAllCaughtUp) {
    // 1. Calculate aligned offset for request
    // marginFileOffset aligns down to segment boundary
    marginOffset = Binlog.marginFileOffset(lastUpdatedOffset)
    
    // 2. Request next binlog segment
    // Send: (marginOffset, lastUpdatedOffset)
    MasterSlave catch_up(fileIndex, marginOffset, lastUpdatedOffset)

    // 3. Master reads segment at aligned offset
    // Returns segment bytes (may be partial if last segment)
    
    // 4. Calculate skip bytes for already-processed data
    skipBytesN = lastUpdatedOffset - marginOffset
    
    // 5. Decode and apply (skip already-processed bytes)
    Binlog.decodeAndApply(segmentBytes, skipBytesN, replPair)
    
    // 6. Update offset
    lastUpdatedOffset = fetchedOffset + readSegmentLength
    
    // 7. Wait interval
    Thread.sleep(catchUpIntervalMillis)
}

if (isAllCaughtUp) {
    state = ReplState.UP_TO_DATE;
}
```

**Offset Alignment Mechanism:**

The catch-up protocol uses a two-offset system to handle segment alignment:

```
Slave State:
  lastUpdatedOffset = exact byte position processed (e.g., 100 bytes into segment)
  
Request to Master:
  needFetchOffset = marginFileOffset(lastUpdatedOffset)  // Aligns DOWN to segment boundary
  lastUpdatedOffset = exact position for skip calculation

Example Flow:
  1. Segment size = 256KB, slave processed 100 bytes
  2. lastUpdatedOffset = X + 100
  3. Request: needFetchOffset = X (aligned), lastUpdatedOffset = X + 100
  4. Master reads segment starting at X
  5. Slave calculates: skipBytesN = (X + 100) - X = 100
  6. Slave skips first 100 bytes, applies remaining content
  7. Updates: lastUpdatedOffset = X + readSegmentLength

Partial Segment Handling:
  - Last segment may be < full segment size (e.g., 100 bytes vs 256KB)
  - readSegmentBytes.length reflects actual bytes
  - Next request: marginOffset aligns to segment start
  - skipBytesN prevents re-processing
  - One extra round-trip occurs but data remains consistent
```

### State Machine

```
┌──────────┐
│ INIT     │
└────┬─────┘
     │
     │ (HELLO/HI)
     ▼
┌──────────┐
│ PRE_CATCHUP (exists_wal, exists_chunk, exists_dict)
└────┬─────┘
     │
     │ (exists_all_done)
     ▼
┌──────────┐
│ CATCH_UP (s_catch_up loop)
└────┬─────┘
     │
     │ (slaveOffset >= masterOffset)
     ▼
┌──────────┐
│ UP_TO_DATE (periodic catch_up)
└────┬─────┘
     │
     │ (connection error / reset)
     ▼
┌──────────┐
│ RECONNECT (re-establish connection)
└──────────┘
```

---

## Replication Protocol

### Message Types

| Type | Code | Direction | Purpose |
|------|------|-----------|---------|
| ping | -1 | Slave→Master | Keepalive |
| pong | 1 | Master→Slave | Keepalive response |
| hello | 2 | Slave→Master | Handshake, identify slave |
| hi | 3 | Master→Slave | Handshake response, identify master |
| bye | 4 | Slave→Master | Start teardown |
| byeBye | 5 | Master→Slave | Teardown complete |
| exists_wal | 19 | Slave→Master | Fetch existing WAL entries |
| exists_chunk_segments | 20 | Slave→Master | Fetch chunk segments |
| exists_big_string | 21 | Slave→Master | Fetch big strings |
| exists_short_string | 22 | Slave→Master | Fetch short strings |
| exists_dict | 25 | Slave→Master | Fetch dictionaries |
| exists_all_done | 26 | Slave→Master | End pre-catchup |
| catch_up | 27 | Slave→Master | Incremental binlog request |
| s_exists_wal | 29 | Master→Slave | WAL entries response |
| s_exists_chunk_segments | 30 | Master→Slave | Chunk segments response |
| s_exists_big_string | 31 | Master→Slave | Big strings response |
| s_exists_short_string | 32 | Master→Slave | Short strings response |
| s_exists_dict | 35 | Master→Slave | Dictionaries response |
| s_exists_all_done | 36 | Master→Slave | Pre-catchup done response |
| s_catch_up | 37 | Master→Slave | Binlog segment response |
| incremental_big_string | 41 | Slave→Master | Fetch incremental big strings |
| s_incremental_big_string | 51 | Master→Slave | Incremental big strings response |
| error | -100 | Master→Slave | Error message |

### Protocol Encoding

```java
public class Repl {
    public static byte[] encode(ReplType type, byte[] content) {
        // Header: X-REPL (6B) + slave_uuid (8B) + slot (2B) + type (1B) + length (4B)
        ByteBuf buf = Unpooled.buffer(21);

        buf.writeBytes("X-REPL".getBytes());
        buf.writeLong(slaveUuid);
        buf.writeShort(slot);
        buf.writeByte(type.code);
        buf.writeInt(content.length);
        buf.writeBytes(content);

        byte[] result = new byte[buf.readableBytes()];
        buf.readBytes(result);
        return result;
    }

    public static ReplRequest decode(byte[] header) {
        // Parse header (21 bytes)
        if (header.length < 21) {
            throw new MalformedDataException("Header too short");
        }

        if (!isXReplKeyword(header)) {
            throw new MalformedDataException("Invalid X-REPL keyword");
        }

        long slaveUuid = Utils.readLong(header, 6);
        short slot = Utils.readShort(header, 14);
        byte typeCode = header[16];
        int contentLength = Utils.readInt(header, 17);

        ReplType type = ReplType.fromCode(typeCode);
        if (type == null) {
            throw new MalformedDataException("Unknown ReplType: " + typeCode);
        }

        return new ReplRequest(slaveUuid, slot, type, contentLength);
    }
}
```

---

## Failover

### Leader Election (ZooKeeper)

```java
public class LeaderSelector {
    private final CuratorFramework zkf;
    private final LeadershipLatch leadershipLatch;
    private String leaderPath;
    private final AtomicBoolean isLeader;

    public void start() {
        leaderPath = zookeeperRootPath + "/LEADER_LATCH_PATH";

        leadershipLatch = new LeadershipLatch(zkf, leaderPath);

        leadershipLatch.addListener(new LeadershipLatchListener() {
            @Override
            public void isLeader() {
                isLeader.set(true);
                becomeMaster();
            }

            @Override
            public void notLeader() {
                isLeader.set(false);
                becomeSlave();
            }
        });

        leadershipLatch.start();
    }

    private void becomeMaster() {
        log.info("Became master");

        // Update configuration
        for (OneSlot slot : LocalPersist.getInstance().oneSlots) {
            slot.resetAsMaster();
        }

        // Publish master info
        // ...
    }

    private void becomeSlave() {
        log.info("Became slave");

        // Connect to new master
        // ...
    }
}
```

### Failover Conditions

**Slave triggers failover when:**

```java
public class ReplPair {
    public void checkFailover() {
        // 1. Check master connection
        if (!isLinkUp()) {
            // Master not responding for >3 seconds
            handleMasterNotAvailable();
            return;
        }

        // 2. Check if master is readonly
        if (isMasterReadonly) {
            // Master is readonly, okay to promote
            if (isAllCaughtUp()) {
                promoteToMaster();
            }
        }

        // 3. Check if self can't connect to master
        if (isMasterCanNotConnect) {
            // We're disconnected
            handleConnectionLoss();
        }
    }

    private void promoteToMaster() {
        // 1. Set leader latch
        leadershipLatch.tryAcquire();

        // 2. Reset slot as master
        slot.resetAsMaster();

        // 3. Publish master change
        // ...

        log.warn("Promoted to master");
    }
}
```

### Auto-Failover (Redis Sentinel Compatible)

Velo supports **Redis Sentinel-style** failover:

```
Sentinel Configuration:
  sentinel monitor mymaster 127.0.0.1 7379 2
  sentinel down-after-milliseconds mymaster 5000
  sentinel failover-timeout mymaster 60000
  sentinel parallel-syncs mymaster 1
  sentinel auth-pass mymaster mypassword
```

**Failover Process:**
1. Sentinels detect master down
2. Sentinels agree new master via election
3. New master promotes (becomes writable)
4. Other slaves sync to new master
5. Client applications reconfigure

---

## Configuration Compatibility

### ReplProperties Record

```java
public record ReplProperties(
    int bucketsPerSlot,
    int oneChargeBucketNumber,
    int segmentNumberPerFd,
    int fdPerChunk,
    int segmentLength,
    boolean isSegmentUseCompression
) {
    public static ReplProperties fromConfForSlot(ConfForSlot conf) {
        return new ReplProperties(
            conf.confBucket.bucketsPerSlot,
            conf.confWal.oneChargeBucketNumber,
            conf.confChunk.segmentNumberPerFd,
            conf.confChunk.fdPerChunk,
            conf.confChunk.segmentLength,
            conf.confChunk.isSegmentUseCompression
        );
    }
}
```

### Slave Compatibility Check

```java
public class ReplPair {
    public boolean slaveCanMatch(ReplProperties masterProps, ReplProperties slaveProps) {
        // All properties must match
        return masterProps.bucketsPerSlot() == slaveProps.bucketsPerSlot() &&
               masterProps.oneChargeBucketNumber() == slaveProps.oneChargeBucketNumber() &&
               masterProps.segmentNumberPerFd() == slaveProps.segmentNumberPerFd() &&
               masterProps.fdPerChunk() == slaveProps.fdPerChunk() &&
               masterProps.segmentLength() == slaveProps.segmentLength() &&
               masterProps.isSegmentUseCompression() == slaveProps.isSegmentUseCompression();
    }

    public void resetAsSlave() {
        // Get master properties via handshake
        ReplProperties masterProps = getMasterProperties();

        // Check local configuration
        ReplProperties localProps = slot.getConfForSlot().generateReplProperties();

        // Verify compatibility
        if (!slaveCanMatch(masterProps, localProps)) {
            log.error("Configuration mismatch with master");
            log.error("Master: {}", masterProps);
            log.error("Local: {}", localProps);
            throw new ReplConfigMismatchException();
        }

        // Start sync
        startCatchUp();
    }
}
```

---

## Related Documentation

### Design Documents
- [Overall Architecture](./01_overall_architecture.md) - System overview
- [Persistence Layer Design](./02_persist_layer_design.md) - Binlog and storage
- [Server Bootstrap Design](./12_server_bootstrap_design.md) - Replication initialization

### Existing Documentation
- [doc/repl/README.md](/home/kerry/ws/velo/doc/repl/README.md) - Replication overview

### Key Source Files
**Replication Core:**
- `src/main/java/io/velo/repl/Repl.java` - Protocol encoding/decoding
- `src/main/java/io/velo/repl/Binlog.java` - Binlog management
- `src/main/java/io/velo/repl/ReplPair.java` - Master-slave pair
- `src/main/java/io/velo/repl/ReplType.java` - Message types

**Failover:**
- `src/main/java/io/velo/repl/LeaderSelector.java` - Leader election
- `src/main/java/io/velo/repl/TcpClient.java` - Slave TCP client

**Content:**
- `src/main/java/io/velo/repl/incremental/XWalV.java` - WAL entry replication
- `src/main/java/io/velo/repl/incremental/XDict.java` - Dictionary replication

---

**Document Version:** 1.1
**Last Updated:** 2026-03-20
**Author:** Velo Architecture Team
**Changelog (2026-03-20):**
- Fixed DYN_CONFIG code from Byte.MAX_VALUE to 121
- Fixed FLUSH code from Byte.MAX_VALUE to -128
- Updated XWalV Entry Format to include encodedLength, isValueShort, and expireAt fields
- Fixed bucketIndex type from short to int
