# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Project Overview

Velo is a Redis protocol compatible, low-latency, hash-index based, slot-sharding, multi-threaded key-value storage system. Written in Java 21 with Groovy for tests, dynamic commands, and cluster management. Uses ActiveJ as the async framework.

## Preparation

- Need installed JDK 21
- Need installed Gradle 8.14+

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

# Run with memory limit
java -Xmx8g -Xms8g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar
```

### Test Execution Notes

- Tests use Spock framework (Groovy-based) with JUnit 5 platform
- Max heap size: 8G for tests
- Fork every: 1 test (isolated test execution)
- Standard streams are shown during test execution
- Test coverage is generated automatically after tests

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

## Code Style Guidelines

### Java Code Style

#### Imports

- Use `java.*` imports first, then `javax.*`, then third-party, then project imports
- Avoid wildcard imports (`import java.util.*;`)
- Do not use fully qualified class names in code when a normal import is appropriate
- Group imports by type with blank lines between groups
- Remove unused imports

#### Formatting

- Use 4 spaces for indentation (no tabs)
- Line length: 120 characters preferred
- Opening braces on same line for classes/methods, new line for control structures
- Use modern Java features (`var`, records, streams) where appropriate

#### Naming Conventions

- Classes: `PascalCase` (e.g., `BaseCommand`, `LocalPersist`)
- Methods: `camelCase` (e.g., `parseSlots`, `handleRequest`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `KEY_MAX_LENGTH`, `VALUE_MAX_LENGTH`)
- Variables: `camelCase` (e.g., `slotNumber`, `keyHash`)
- Packages: `lowercase` (e.g., `io.velo.command`)

#### Documentation

- Public classes and methods should have JavaDoc comments
- Use `@param`, `@return`, and `@throws` tags appropriately
- Keep comments concise and focused on the why, not the what

#### Error Handling

- Use custom exceptions that extend `RuntimeException`
- Exception classes should be descriptive (e.g., `SegmentOverflowException`, `TypeMismatchException`)
- Log errors using SLF4J with appropriate levels
- Handle exceptions gracefully and provide meaningful error responses

### Groovy Code Style

#### General

- Use `CompileStatic` transformation (configured in `config.groovy`)
- Follow Java naming conventions for consistency
- Prefer imports over fully qualified class names in code and tests
- Use type annotations where beneficial for performance
- Use Groovy styles for simpler code (refer to `src/test/groovy/GroovyStyleRefer.groovy`)

#### Testing (Spock)

- Test classes extend `Specification`
- Use given-when-then or expect-where blocks
- Test methods use descriptive strings with spaces (e.g., `def 'test static methods'()`)
- Arrange tests in logical order with clear setup and teardown

## Code Conventions

- Commit messages: Keep short and simple, follow history style (e.g., "fix: use slot parse short when repl.")
- Java: 4-space indent, 120-char lines, PascalCase classes, camelCase methods, UPPER_SNAKE_CASE constants
- Groovy tests: Spock `Specification` with descriptive string method names (e.g., `def 'test static methods'()`)
- Groovy is compiled with `@CompileStatic` by default (`config.groovy`)
- Refer to `src/test/groovy/GroovyStyleRefer.groovy` for Groovy style patterns
- Commands follow the alphabetical group pattern — add new commands to the appropriate letter group
- Follow TDD for every feature, bug fix, refactor, and behavior change: write the test first, verify it fails for the expected reason, then write the minimal code to pass
- After running relevant tests for a change, inspect the JaCoCo HTML report and confirm the changed code lines or branches were actually executed

## Development Workflow

### Before Making Changes

1. Run existing tests related to the changed code to ensure baseline functionality
2. Understand the command structure and slot handling
3. Check for similar implementations in existing code
4. Consider performance implications
5. Consider multi-threading considerations
6. Read `doc/` because there are design documents there

### After Making Changes

1. Run relevant tests: `./gradlew test --tests "*YourTest*"`
2. Run the full test suite if changes are significant
3. Check code coverage with JaCoCo and warn if coverage is below 80%, then consider adding more tests
4. Inspect the JaCoCo HTML report and confirm the changed lines or branches were actually executed; a passing test alone is not enough
5. Test Redis protocol compatibility if applicable
6. Verify performance with JMH benchmarks if relevant

### Testing Guidelines

- Write comprehensive tests for new functionality
- Use Spock's data-driven testing for multiple scenarios
- Test edge cases and error conditions
- Mock external dependencies appropriately
- Reuse as much of the existing test code as possible
- Test thread-safety for concurrent operations
- After running tests for a change, check the JaCoCo report for the touched classes and make sure the changed code path was executed

## Configuration

- `velo.properties` — Main app config (slot count, workers, listen address, LRU size, etc.)
- `log4j2.xml` — Logging
- `acl.conf` — ACL rules

## Bug Reviews Workflow

Bug review documents live under `doc/bug_reviews/`. File naming format: `bug_<N>_<module>_review_round_<R>.md` (e.g., `bug_8_replication_module_review_round_3.md`).

Two-agent collaboration workflow:

1. **AI agent 1 (author)** — Creates a new bug review doc in `doc/bug_reviews/` following the file-name format above. Each bug entry should include: severity, cited files with line ranges, a code excerpt, and a description of the root cause and impact.
2. **AI agent 2 (reviewer)** — Reads the doc, verifies each bug against the current code, and updates the same doc with review notes (e.g., confirming, refuting, or refining each finding).
3. **One AI agent (1 or 2)** — Implements fixes for the confirmed bugs one by one. Each bug fix must follow TDD, must be verified with relevant tests plus JaCoCo coverage inspection, and must end with its own commit before moving to the next bug.
4. **Another AI agent** — Reviews the committed fix, then appends a "Review Feedback" section to the same doc covering: summary of the fix, strengths, concerns, and pre-commit/post-commit follow-ups.

Keep the same doc as the single source of truth across all rounds — append new sections rather than creating parallel files.

### Bug Fix Execution Workflow

Once bugs are confirmed in the review doc, execute fixes in this sequence:

1. Select one confirmed bug only.
2. Add or update the relevant test first.
3. Run the relevant test and confirm it fails for the expected reason.
4. Implement the minimal production change for that bug.
5. Re-run the relevant tests and confirm they pass.
6. Inspect the JaCoCo HTML report and confirm the touched lines or branches were executed.
7. Commit that single bug fix.
8. Repeat for the next confirmed bug.

Rules:
- No production code before the failing test for that bug exists.
- A passing test is not enough; JaCoCo confirmation is required before claiming the fix is complete.
- Keep commits scoped to one bug fix unless the user explicitly requests batching.

> **Tips for bug reviews**: When reviewing bugs in a module, create a new branch based on `main` and name it `review/<module-type>` (e.g., `review/persist`, `review/replication`). The module name can be inferred from existing docs under `doc/design/*.md` (e.g., `doc/design/02_persist_layer_design.md` indicates the `persist` module).

## Common Patterns

### Command Implementation

```java
public class YourCommand extends BaseCommand {
    @Override
    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        // Parse slot information
    }

    @Override
    public Reply handle() {
        // Implement command logic
    }
}
```

### Error Handling

```java
try{
        // Operation
        }catch(SpecificException e){
        log.

error("Operation failed",e);
    return Reply.

error("ERR operation failed");
}
```

### Testing Pattern

```groovy
def 'test your functionality'() {
    given:
    def setup = createTestSetup()

    when:
    def result = setup.executeOperation()

    then:
    result == expectedValue
}
```

## Performance Optimization

### Memory Management

- Use `BigStringNoMemoryCopy` for large string operations
- Implement efficient byte buffer handling
- Consider off-heap storage for large datasets

### Concurrency

- Use `@ThreadNeedLocal` annotation for thread-local requirements
- Implement proper synchronization for shared resources
- Consider lock-free patterns for high-throughput operations

### Compression

- Use Zstd compression with trained dictionaries
- Implement adaptive compression strategies
- Balance compression ratio vs. performance

## Redis Protocol Compatibility

- Maintain strict Redis protocol compatibility
- Handle RESP (Redis Serialization Protocol) correctly
- Support standard Redis command patterns
- Implement proper error responses in Redis format

## Monitoring and Metrics

- Use Prometheus metrics for monitoring
- Implement custom gauges and counters
- Log performance metrics appropriately
- Monitor memory usage and GC behavior

## Security Considerations

- Implement proper access control with ACL
- Validate input data to prevent injection
- Use secure defaults for configuration
- Consider rate limiting for operations

## Key Dependencies

- ActiveJ 6.0-beta2 (async framework, event loop, HTTP, DI)
- Zstd-jni (compression with trainable dictionaries)
- Spock 2.3 / Groovy 4.0.12 (testing)
- Jedis 4.4.7 (Redis client for testing)
- RocksDB 9.7.3 (key analysis only, not main storage)
- Apache Curator (ZooKeeper for cluster coordination)
- Prometheus simpleclient (metrics)

## Build Tools

- Gradle 8.14: Build system
- JMH 1.36: Performance benchmarking
- JaCoCo 0.8.12: Code coverage

## Notes for Agents

1. **Always run tests** before and after changes, but run only relevant tests rather than the entire suite by default
2. **Always check JaCoCo after relevant tests** and confirm the changed lines or branches were executed
3. **Follow existing patterns** rather than introducing new ones
4. **Consider performance implications** of all changes
5. **Maintain Redis compatibility** in all protocol implementations
6. **Test multi-threading scenarios** for concurrent operations
7. **Use proper logging** for debugging and monitoring
8. **Document complex logic** with clear comments
9. **Handle errors gracefully** and provide meaningful messages
10. **Keep commit messages short and simple**, following history style (e.g., `fix: use slot parse short when repl.`)
