# Velo Agent Guidelines

This file contains guidelines for agentic coding agents working on the Velo codebase.

## Project Overview

Velo is a Redis protocol compatible, low-latency, hash index based, slot sharding, multi-threading key-value storage
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
- Use type annotations where beneficial for performance
- Use Groovy styles for simpler code (refer to `src/test/groovy/GroovyStyleRefer.groovy`)

#### Testing (Spock)

- Test classes extend `Specification`
- Use given-when-then or expect-where blocks
- Test methods: use descriptive strings with spaces (e.g., `def 'test static methods'()`)
- Arrange tests in logical order with clear setup and teardown

## Architecture Guidelines

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
4. Test Redis protocol compatibility if applicable
5. Verify performance with JMH benchmarks if relevant

### Testing Guidelines

- Write comprehensive tests for new functionality
- Use Spock's data-driven testing for multiple scenarios
- Test edge cases and error conditions
- Mock external dependencies appropriately
- Use as much as existing test codes
- Test thread-safety for concurrent operations

## Configuration

### Key Configuration Files

- `velo.properties`: Main application configuration
- `log4j2.xml`: Logging configuration
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
- Groovy 4.0.12: Dynamic language support
- Spock 2.3: Testing framework
- RocksDB 9.7.3: Storage engine only for key analysis, not main storage
- Jackson 2.14.3: JSON processing
- Jedis 4.4.7: Redis client

### Build Tools

- Gradle 8.14: Build system
- JMH 1.36: Performance benchmarking
- JaCoCo 0.8.12: Code coverage

## Notes for Agents

1. **Always run tests** before and after changes, do not run all tests, run only relevant tests
2. **Follow existing patterns** rather than introducing new ones
3. **Consider performance implications** of all changes
4. **Maintain Redis compatibility** in all protocol implementations
5. **Test multi-threading scenarios** for concurrent operations
6. **Use proper logging** for debugging and monitoring
7. **Document complex logic** with clear comments
8. **Handle errors gracefully** and provide meaningful messages