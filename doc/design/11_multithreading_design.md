# Velo Multithreading Design

## Overview

Velo uses a **multi-threaded, run-to-complete** architecture for high throughput and low latency.

## Worker Pools

### Network Workers (1-32 threads)

**Purpose:** Handle TCP connections and request parsing

```
NetWorker Thread (1-32):
  ├─> Accept TCP connections
  ├─> Decode requests (RESP/HTTP/X-REPL)
  ├─> Route to slot workers
  └─> Serialize and send replies
```

**Thread-Local Data:**
- Request decoder instances
- Output buffers
- Socket connections

**Configuration:**
```properties
netWorkers=2  # Default: 2
Max: 32
```

### Slot Workers (1-32 threads)

**Purpose:** Execute commands and manage data

```
SlotWorker Thread (1-32):
  ├─> Command groups (A-Z)
  ├─> RequestHandler per worker
  ├─> Slot-owning (modulo)
  └─> Persistence operations
```

**Thread-Local Data:**
- RequestHandler (with command groups)
- Compression contexts (ZstdCompressCtx)
- Decompression contexts (ZstdDecompressCtx)

**Configuration:**
```properties
slotWorkers=4  # Default: 4
Max: 32
```

### Index Workers (1-16 threads)

**Purpose:** Background tasks

```
IndexWorker Thread (1-16):
  ├─> LRU eviction
  ├─> Segment merge
  ├─> Statistics collection
  └─> Key analysis
```

**Configuration:**
```properties
indexWorkers=1  # Default: 1
Max: 16
```

## Slot Assignment

### Distribution Formula

```
slotWorker = slotIndex % slotWorkers

Example:
  slotNumber = 4
  slotWorkers = 2
  
  Slot 0 → Worker 0
  Slot 1 → Worker 1
  Slot 2 → Worker 0
  Slot 3 → Worker 1
```

### Thread-Local Pattern

```java
@ThreadNeedLocal("slot")
RequestHandler[] requestHandlerArray;

// Access pattern
int workerId = Thread.currentThread().getId();
RequestHandler rh = requestHandlerArray[workerId];
rh.handle(request);
```

## Run-to-Complete Model

Each request processed by a single thread from start to finish:

```
┌─────────────────────────────────────────┐
│         Network Worker Thread           │
│  1. Accept TCP connection              │
│  2. Decode request                     │
│  3. Route to slot worker               │
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│          Slot Worker Thread              │
│  4. Parse slots                        │
│  5. Select command group               │
│  6. Execute command                     │
│  7. Read/write persistence              │
│  8. Build reply                         │
└─────────────┬───────────────────────────┘
              │
              ▼
┌─────────────────────────────────────────┐
│         Network Worker Thread           │
│  9. Serialize reply                     │
│  10. Send to client                     │
└──────────────────────────────────────────┘
```

## Cross-Slot Operations

Commands touching multiple slots:

```
Strategy: Async with promises

MGET key1 key2 key3 (different slots)
  ├─> Promise 1: GET key1 (slot 0, worker A)
  ├─> Promise 2: GET key2 (slot 1, worker B)
  ├─> Promise 3: GET key3 (slot 0, worker A)
  └─> Promises.all() → Combine results
```

Implementation:
```java
if (!isCrossRequestWorker) {
    // Single thread path
    operateSet(set, otherRhkList, isInter, isUnion);
} else {
    // Async path
    ArrayList<Promise<RedisHashKeys>> promises = new ArrayList<>();
    for (int i = 1; i < list.size(); i++) {
        Promise<RedisHashKeys> p = slot.asyncCall(() -> getRedisSet(other));
        promises.add(p);
    }
    Promises.all(promises).whenComplete(replies -> {
        // Combine replies
    });
}
```

## Synchronization Avoidance

### No-Lock Design

```
Slot-Local Data:
├─> OneSlot instances (owned by worker)
├─> KeyBuckets (worker-local file handles)
├─> Chunk segments (worker-local I/O)
└─> WAL HashMaps (worker-local)

Result: No synchronization needed for slot operations
```

### Shared-State Synchronization

```java
// Minimal synchronization for global resources
class DictMap {
    private final ConcurrentHashMap<Integer, Dict> dictBySeq;
    // Concurrent map for thread-safe access
}

// Thread-local for performance
class Dict {
    @ThreadNeedLocal("slot")
    private static ZstdCompressCtx[] ctxCompressArray;
}
```

## ActiveJ Event Loops

Each slot worker has an **Eventloop** for async operations:

```java
// OneSlot initialization
Eventloop eventloop = Eventloop.create()
    .withIdlePing(true)
    .withThreadPriority(Thread.NORM_PRIORITY);

// Task submission
Promise<Reply> p = eventloop.execute(() -> {
    return command.handle();
});

// Task with delay
eventloop.delay(taskIntervalMillis, eventloop::execute);
```

## Worker Lifecycle

```
┌────────────┐
│  Startup   │
└─────┬──────┘
      │
      ├─> Create worker threads
      ├─> Initialize thread-local data
      ├─> Create RequestHandlers
      ├─> Start event loops
      └─> Begin accepting connections
           │
           ▼
┌────────────┐
│    Ready    │
│  (Running)  │
└─────┬──────┘
      │
      ├─> Accept connections
      ├─> Process requests
      └─> Handle commands
           │
           ▼
┌────────────┐
│  Shutdown  │
└────────────┘
```

## Performance Considerations

### Thread Count Recommendations

| Node Type | Net Workers | Slot Workers | Index Workers |
|-----------|-------------|---------------|----------------|
| Small (4 cores) | 1-2 | 2-4 | 1 |
| Medium (8 cores) | 2-4 | 4-8 | 2 |
| Large (16 cores) | 4-8 | 8-16 | 4 |

Formula:
```
Total threads ≈ CPU cores * 1.5

Recommended:
  slotWorkers = (CPU cores * 0.6).toInt()
  netWorkers = (CPU cores * 0.25).toInt()
  indexWorkers = (CPU cores * 0.15).toInt()
```

## Related Documentation

- [Overall Architecture](./01_overall_architecture.md) - System overview
- [Command Processing](./04_command_processing_design.md) - Cross-slot coordination
- [Persistence Layer](./02_persist_layer_design.md) - Thread-safe storage

## Key Source Files

- `src/main/java/io/velo/ThreadNeedLocal.java` - Thread-local annotation
- `src/main/java/io/velo/RequestHandler.java` - Per-worker handler
- `src/main/java/io/velo/MultiWorkerServer.java` - Server startup

---

**Version:** 1.0  
**Last Updated:** 2025-02-05  
