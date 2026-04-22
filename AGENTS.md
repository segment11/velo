# Velo Agent Guidelines

This file contains guidelines for agentic coding agents working on the Velo codebase.

## Project Overview

Velo is a Redis protocol compatible, http protocol supported, low-latency, hash index based, slot sharding,
multi-threading key-value storage
system written in Java with Groovy components.

## Preparation

- Need installed JDK 21
- Need installed Gradle 8.14+

## Build Commands

### Core Commands

```bash
# Build the project
./gradlew jar

# Run all tests
./gradlew test

# Run a single test class
./gradlew test --tests "io.velo.UtilsTest"

# Run a single test method (Spock/Groovy)
./gradlew test --tests "io.velo.UtilsTest.test base"

# Run tests with specific pattern
./gradlew test --tests "*Test"

# Clean build artifacts
./gradlew clean

# Create fat JAR for deployment
./gradlew fatJar

# Run JMH benchmarks
./gradlew jmh

# Run specific JMH benchmark
./gradlew jmh -Pjmh.includes="SlotCalc*"

# Generate test coverage report
./gradlew jacocoTestReport

# Run after build
cd build/libs && java -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar

# Run with memory limit
java -Xmx8g -Xms8g -XX:+UseZGC -XX:+ZGenerational -XX:MaxDirectMemorySize=64m -jar velo-1.0.0.jar
```

### Test Execution Notes

- Tests use Spock framework (Groovy-based) with JUnit 5 platform
- Max heap size: 8G for tests
- Fork every: 1 test (isolated test execution)
- Standard streams are shown during test execution
- Test coverage is generated automatically after tests

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
- Use modern Java features (var, records, streams) where appropriate

#### Naming Conventions

- Classes: `PascalCase` (e.g., `BaseCommand`, `LocalPersist`)
- Methods: `camelCase` (e.g., `parseSlots`, `handleRequest`)
- Constants: `UPPER_SNAKE_CASE` (e.g., `KEY_MAX_LENGTH`, `VALUE_MAX_LENGTH`)
- Variables: `camelCase` (e.g., `slotNumber`, `keyHash`)
- Packages: `lowercase` (e.g., `io.velo.command`)

#### Documentation

- Public classes and methods should have JavaDoc comments
- Use `@param`, `@return`, and `@throws` tags appropriately
- Keep comments concise and focused on the "why" not the "what"

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
- Test methods: use descriptive strings with spaces (e.g., `def 'test static methods'()`)
- Arrange tests in logical order with clear setup and teardown

## Architecture Guidelines

### Multi-module Structure
- **velo** (root): Main project
- **segment_common**: Shared utilities submodule
- **segmentweb**: Web framework submodule

### Source Layout
- `src/main/java/io/velo/` — Core Java source
- `src/main/groovy/` — Groovy source (cluster watch/failover management)
- `src/test/groovy/` — Tests (Spock framework)
- `dyn/src/` — Hot-reloadable Groovy commands (cluster, config, info, manage, extend)
- `dyn/test/` — Tests for dynamic commands

### Threading Model
- Network worker threads handle connections
- Slot worker threads handle data operations (thread-local data, run-to-completion model)
- `@ThreadNeedLocal` annotation marks thread-local requirements

### Entry Point
`io.velo.MultiWorkerServer` — main class using ActiveJ launcher

### Package Structure

```
io.velo/             # Main package
├── acl/             # Redis Access control models
├── command/         # Redis commands implementations, group by first letter (A-Z groups)
├── decode/          # RESP request / replication request / http request decoding
├── dyn/             # Some commands are implemented in groovy, use CachedGroovyClassLoader to dyn load last updated groovy file, hot-reload
├── extend/          # Extensions, ignored
├—— ingest/          # Ingestion parquet files, reader
├── metric/          # Metrics adapter to prometheus
├── mock/            # Mock persist layers, use in memory HashMap for testing
├── monitor/         # Monitoring some metrics when running, like top big keys, cpu usage 
├── perf/            # Perf test examples
├── persist/         # Persistence layer, hash index implementations
├── rdb/             # Redis RDB file format read / write, for dump / restore
├── repl/            # Replication functionality
├── reply/           # Redis protocol reply abstraction
├── task/            # Interval execute tasks, delay in an event loop
└── type/            # Redis data types
```

TIPS: Main packet io.velo features are:

- CompressedValue abstraction
- Zstd dictionary training and management
- Configuration management, for global and for each slot (hash index's sharding)
- Annotations for better readability
- Server Launcher
- Request Handler
- Hash calculation

src/main/groovy:

This is a separate module, for Velo cluster management, like migration, failover, etc.

Some manage commands are implemented in groovy in dir /dyn/src/io/velo/command, now it has
'cluster', 'config', 'command', 'info', 'manage' etc.
User can add new commands besides redis existing commands. Take ManageCommand as example.

### Key Patterns

- Commands are organized alphabetically (AGroup, BGroup, etc.)
- Use abstract base classes for common functionality
- Implement interfaces for extensibility
- Use dependency injection with ActiveJ framework
- Thread-local patterns for multi-threading support

### Performance Considerations

- Use `ByteBuffer` for binary data operations
- Implement efficient memory management for large datasets
- Use compression (Zstd) for value storage
- Optimize for Redis protocol compatibility
- Consider slot-based sharding for scalability

## Development Workflow

### Before Making Changes

1. Run existing tests related to the changes codes to ensure baseline functionality
2. Understand the command structure and slot handling
3. Check for similar implementations in existing code
4. Consider performance implications
5. Consider multi-threading considerations
6. Read dir /doc, there are some design documents

### After Making Changes

1. Run relevant tests: `./gradlew test --tests "*YourTest*"`
2. Run full test suite if changes are significant
3. Check code coverage with JaCoCo report, if coverage is below 80%, show warnings and consider adding more tests
4. After running relevant tests, inspect the JaCoCo HTML report and confirm the changed code lines or branches were actually executed; a passing test alone is not enough
5. Test Redis protocol compatibility if applicable
6. Verify performance with JMH benchmarks if relevant

### Testing Guidelines

- Follow TDD for every feature, bug fix, refactor, and behavior change: write the test first, run it and verify it fails for the expected reason, then implement the minimal code to make it pass
- Write comprehensive tests for new functionality
- Use Spock's data-driven testing for multiple scenarios
- Test edge cases and error conditions
- Mock external dependencies appropriately
- Use as much as existing test codes
- Test thread-safety for concurrent operations
- After running tests for a change, check the JaCoCo report for the touched classes and make sure the changed code path was executed

## Bug Reviews Workflow

Bug review documents live under `doc/bug_reviews/`. File naming format: `bug_<N>_<module>_review_round_<R>.md` (e.g., `bug_8_replication_module_review_round_3.md`).

Two-agent collaboration workflow:

1. **AI agent 1 (author)** — Creates a new bug review doc in `doc/bug_reviews/` following the file-name format above. Each bug entry should include: severity, cited files with line ranges, a code excerpt, and a description of the root cause and impact.
2. **AI agent 2 (reviewer)** — Reads the doc, verifies each bug against the current code, and updates the same doc with review notes (e.g., confirming, refuting, or refining each finding).
3. **One AI agent (1 or 2)** — Implements fixes for the confirmed bugs one by one. Each bug fix must follow TDD, must be verified with relevant tests plus JaCoCo coverage inspection, and must end with its own commit before moving to the next bug.
4. **Another AI agent** — Reviews the committed fix, then appends a "Review Feedback" section to the same doc covering: summary of the fix, strengths, concerns, and pre-commit/post-commit follow-ups.

Keep the same doc as the single source of truth across all rounds — append new sections rather than creating parallel files.

### Bug Fix Execution Workflow

After bugs are confirmed in the review doc, fix them in this order and do not batch unrelated confirmed bugs into one commit:

1. Pick one confirmed bug.
2. Add or update the relevant test first.
3. Run the relevant test and confirm it fails for the expected reason.
4. Implement the minimal production code change for that single bug.
5. Re-run the relevant tests and confirm they pass.
6. Inspect the JaCoCo HTML report and confirm the changed lines or branches for that bug were actually executed.
7. Commit that single bug fix with a short commit message.
8. Move to the next confirmed bug and repeat from step 1.

Notes:
- Do not start production code for a bug fix before the failing test exists.
- Do not claim a bug is fixed from a green test alone; JaCoCo confirmation is also required.
- If multiple bugs touch the same area, still keep review, test intent, and commits separated bug by bug unless the user explicitly asks for batching.

> **Tips for bug reviews**: When reviewing bugs in a module, create a new branch based on `main` and name it `review/<module-type>` (e.g., `review/persist`, `review/replication`). The module name can be inferred from existing docs under `doc/design/*.md` (e.g., `doc/design/02_persist_layer_design.md` indicates the `persist` module).

## Configuration

### Key Configuration Files

- `velo.properties`: Main application configuration
- `log4j2.xml`: Logging configuration
- `acl.conf`: ACL rules
- `build.gradle`: Build configuration and dependencies

### Environment Setup

- JDK 21 required
- Gradle 8.14+ (wrapper provided)
- Redis client tools for testing protocol compatibility

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

## Tools and Frameworks

### Core Dependencies

- ActiveJ 6.0-beta2: Async framework
- Zstd-jni: Compression with trainable dictionaries
- Groovy 4.0.12: Dynamic language support
- Spock 2.3: Testing framework
- Jedis 4.4.7: Redis client
- RocksDB 9.7.3: Key analysis only, not main storage
- Apache Curator: ZooKeeper for cluster coordination
- Prometheus simpleclient: Metrics

### Build Tools

- Gradle 8.14: Build system
- JMH 1.36: Performance benchmarking
- JaCoCo 0.8.12: Code coverage

## Notes for Agents

1. **Always run tests** before and after changes, do not run all tests, run only relevant tests
2. **Always check JaCoCo after relevant tests** and confirm the changed lines or branches were executed
3. **Follow existing patterns** rather than introducing new ones
4. **Consider performance implications** of all changes
5. **Maintain Redis compatibility** in all protocol implementations
6. **Test multi-threading scenarios** for concurrent operations
7. **Use proper logging** for debugging and monitoring
8. **Document complex logic** with clear comments
9. **Handle errors gracefully** and provide meaningful messages
10. **Keep commit messages short and simple**, follow history style (e.g., "fix: use slot parse short when repl.")
