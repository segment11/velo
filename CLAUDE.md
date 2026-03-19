# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Velo is a Redis protocol compatible, low-latency, hash-index based, slot-sharding, multi-threaded key-value storage system. Written in Java 21 with Groovy for tests, dynamic commands, and cluster management. Uses ActiveJ as the async framework.

## Build & Test Commands

```bash
# Build
./gradlew jar                  # Build JAR (also copies deps, configs, dyn scripts)
./gradlew fatJar               # Fat JAR for deployment
./gradlew clean                # Clean build artifacts

# Test (Spock framework on JUnit 5, forkEvery=1, maxHeap=8G)
./gradlew test                                          # Run all tests
./gradlew test --tests "io.velo.UtilsTest"              # Single test class
./gradlew test --tests "io.velo.UtilsTest.test base"    # Single test method
./gradlew test --tests "*GroupTest"                      # Pattern match

# Coverage & benchmarks
./gradlew jacocoTestReport     # Coverage report (auto-runs after test)
./gradlew jmh                  # JMH benchmarks

# Run
cd build/libs && java -Xmx1g -Xms1g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar
```

## Architecture

### Multi-module structure
- **velo** (root): Main project
- **segment_common**: Shared utilities submodule
- **segmentweb**: Web framework submodule

### Source layout
- `src/main/java/io/velo/` — Core Java source
- `src/main/groovy/` — Groovy source (cluster watch/failover management)
- `src/test/groovy/` — Tests (Spock framework)
- `dyn/src/` — Hot-reloadable Groovy commands (cluster, config, info, manage, extend)
- `dyn/test/` — Tests for dynamic commands

### Key packages (`io.velo`)
- `command/` — Redis command implementations, organized alphabetically (AGroup, BGroup, ... ZGroup). Each group extends `BaseCommand` with `parseSlots()` and `handle()` methods.
- `persist/` — Three-layer persistence: WAL → Key Buckets → Chunk Segments
- `decode/` — RESP / HTTP / replication protocol decoding
- `repl/` — Master-slave replication and cluster support
- `type/` — Redis data type implementations (string, hash, list, set, zset, bitmap, hyperloglog, bloomfilter, geo)
- `dyn/` — Dynamic Groovy class loading with hot-reload
- `acl/` — Redis ACL access control
- `metric/` — Prometheus metrics
- `rdb/` — Redis RDB format read/write for dump/restore
- `mock/` — In-memory mock persistence for testing
- `CompressedValue` — Core value abstraction with Zstd dictionary-based compression

### Threading model
- Network worker threads handle connections
- Slot worker threads handle data operations (thread-local data, run-to-completion model)
- `@ThreadNeedLocal` annotation marks thread-local requirements

### Entry point
`io.velo.MultiWorkerServer` — main class using ActiveJ launcher

## Code Conventions

- Java: 4-space indent, 120-char lines, PascalCase classes, camelCase methods, UPPER_SNAKE_CASE constants
- Groovy tests: Spock `Specification` with descriptive string method names (e.g., `def 'test static methods'()`)
- Groovy is compiled with `@CompileStatic` by default (`config.groovy`)
- Refer to `src/test/groovy/GroovyStyleRefer.groovy` for Groovy style patterns
- Commands follow the alphabetical group pattern — add new commands to the appropriate letter group
- After running relevant tests for a change, inspect the JaCoCo HTML report and confirm the changed code lines or branches were actually executed

## Configuration

- `velo.properties` — Main app config (slot count, workers, listen address, LRU size, etc.)
- `log4j2.xml` — Logging
- `acl.conf` — ACL rules

## Key Dependencies

- ActiveJ 6.0-beta2 (async framework, event loop, HTTP, DI)
- Zstd-jni (compression with trainable dictionaries)
- Spock 2.3 / Groovy 4.0.12 (testing)
- Jedis 4.4.7 (Redis client for testing)
- RocksDB 9.7.3 (key analysis only, not main storage)
- Apache Curator (ZooKeeper for cluster coordination)
- Prometheus simpleclient (metrics)
