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
├── binlog-0   (Segment 0)
├── binlog-1   (Segment 1)
├── binlog-2   (Segment 2)
├── ...
└── binlog-N   (Segment N)

Each file: ≤ binlogOneFileMaxLength (default: 32MB)
Segment size: binlogOneSegmentLength (default: 262,144 bytes)
Total segments: binlogOneFileMaxLength / binlogOneSegmentLength ≈ 128
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
    DYN_CONFIG(Byte.MAX_VALUE, "XDynConfig - Configuration change"),
    FLUSH(Byte.MIN_VALUE, "XFlush - Flush to persist");

    private final int code;
    private final String description;
}
```

### XWalV Entry Format

```
Type code: 1 (XWalV)

Format:
┌──────────────────────────────────────────────────────────┐
│ type (byte)                                                │
│ seq (long 8B)                                            │
│ bucketIndex (short 2B)                                   │
│ keyHash (long 8B)                                         │
│ spType (int 4B)   - dict seq or special type              │
│ keyLength (short 2B)                                     │
│ keyBytes (variable)                                      │
│ cvEncodedLength (int 4B)                                 │
│ cvEncoded (variable)                                     │
└──────────────────────────────────────────────────────────┘

Example: SET key "value"
  type: 1
  seq: 123456789
  bucketIndex: 1024
  keyHash: 9876543210123456789
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

**Pull Loop:**

```
while (!isAllCaughtUp) {
    // 1. Request next binlog segment
    MasterSlave s_catch_up(fileIndex, fileOffset, segmentLength)

    // 2. Receive binlog segment
    ReplType: s_catch_up (301)
    Content:
      ┌─────────────────────────────────────────┐
      │ segmentIndex (int 4B)                 │
      │ segmentBytes (var)                     │
      └─────────────────────────────────────────┘

    // 3. Decode and apply
    Binlog.decodeAndApply(segmentBytes, replPair)

    // 4. Update offset
    slaveLastCatchUpBinlogFileIndexAndOffset = newOffset

    // 5. Wait interval
    Thread.sleep(catchUpIntervalMillis)
}

// 4. Check if caught up
if (isAllCaughtUp) {
    state = ReplState.UP_TO_DATE;
}
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
| hello | 0 | Slave→Master | Handshake, identify slave |
| hi | 1 | Master→Slave | Handshake response, identify master |
| exists_* | 100-103 | Slave→Master | Pre-catchup requests |
| s_exists_* | 200-203 | Master→Slave | Pre-catchup data |
| exists_all_done | 199 | Slave→Master | End pre-catchup |
| catch_up | 300 | Slave→Master | Incremental binlog request |
| s_catch_up | 301 | Master→Slave | Binlog segment response |
| ping | 400 | Either | Keepalive |
| pong | 401 | Either | Keepalive response |
| bye | 500 | Either | Start teardown |
| byeBye | 501 | Either | Teardown complete |

### Protocol Encoding

```java
public class Repl {
    public static byte[] encode(ReplType type, byte[] content) {
        // Header: X-REPL (6B) + slave_uuid (8B) + slot (2B) + type (2B) + length (4B)
        ByteBuf buf = Unpooled.buffer(22);

        buf.writeBytes("X-REPL".getBytes());
        buf.writeLong(slaveUuid);
        buf.writeShort(slot);
        buf.writeShort(type.code);
        buf.writeInt(content.length);
        buf.writeBytes(content);

        byte[] result = new byte[buf.readableBytes()];
        buf.readBytes(result);
        return result;
    }

    public static ReplRequest decode(byte[] header) {
        // Parse header (22 bytes)
        if (header.length < 22) {
            throw new MalformedDataException("Header too short");
        }

        if (!isXReplKeyword(header)) {
            throw new MalformedDataException("Invalid X-REPL keyword");
        }

        long slaveUuid = Utils.readLong(header, 6);
        short slot = Utils.readShort(header, 14);
        short typeCode = Utils.readShort(header, 16);
        int contentLength = Utils.readInt(header, 18);

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

**Document Version:** 1.0
**Last Updated:** 2025-02-05
**Author:** Velo Architecture Team
