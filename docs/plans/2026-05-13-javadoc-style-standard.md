# JavaDoc Style Standard

## Overview

This document defines the JavaDoc style and format standard for the Velo project. All Java files in `src/main/java/io/velo/**` should follow these guidelines.

## Core Principles

1. **Full codebase coverage** - All Java files, all visibility levels
2. **Minimal for trivial methods** - Getters/setters inherit documentation from field declarations
3. **Custom exceptions only** - Document project-specific exceptions; skip standard JDK exceptions

## JavaDoc Block Structure

### Single-Line Summary (Required)

Every class, interface, and public method must have a summary line.

```java
/**
 * Processes incoming replication requests from slave nodes.
 */
public class ReplRequest { }

/**
 * @param slot the slot index (0-16383)
 * @return the decoded value or null if not found
 */
public byte[] decodeValue(int slot) { }
```

### Multi-Line Description (Optional)

Use only when additional context is genuinely needed (1-2 sentences max).

```java
/**
 * WAL (Write-Ahead Log) entries for crash recovery.
 * Each entry contains the key-value pair metadata and sequence number.
 */
public record WalEntry { }
```

**Rule**: Avoid unnecessary verbosity. If the method name is self-explanatory, the JavaDoc should be brief.

## Required Elements by Type

| Element | Summary | @param | @return | @throws |
|---------|---------|--------|---------|---------|
| Class/Interface | Required | - | - | - |
| Public Constructor | Required | Required | - | Custom only |
| Public Method | Required | Required | Required | Custom only |
| Protected Method | Required | When meaningful | When meaningful | Custom only |
| Private Method | Optional | - | - | - |
| Getter/Setter | Inherit | - | - | - |
| Overridden Method | Optional | - | - | - |

## Parameter Documentation

### Format

```java
/**
 * @param paramName description - be specific about format and constraints
 */
```

### Rules

- Always document all parameters in public methods
- Describe constraints: `"the slot index (0-16383)"`
- Describe units or format: `"timestamp in milliseconds since epoch"`
- Be specific: `"@param key the cache key (non-null)"` not `"@param key the key"`

### Examples

```java
// Good
/**
 * @param timeout connection timeout in milliseconds
 */
public void connect(long timeout) { }

// Bad - too vague
/**
 * @param timeout timeout value
 */
public void connect(long timeout) { }
```

## Return Documentation

### Format

```java
/**
 * @return description of what the caller can expect
 */
```

### Rules

- Required for all non-void public methods
- Trivial getters may omit if the field JavaDoc is clear

```java
/** The configured slot directory. */
File slotDir;

// Getter - JavaDoc optional since it inherits from field
public File getSlotDir() { return slotDir; }
```

## Exception Documentation

### Required

Document custom project exceptions only:

```java
/**
 * @throws SegmentOverflowException when the segment exceeds capacity
 */
public void addEntry(Entry e) { }
```

### Skip

Do NOT document standard JDK exceptions:

- `IllegalArgumentException`
- `NullPointerException`
- `IllegalStateException`
- `IOException` / `EOFException`
- `RuntimeException`

These are self-explanatory from the method signature and context.

### Custom Exceptions to Document

Project exceptions that should be documented:

- `SegmentOverflowException`
- `SegmentOverflowException`
- `BucketFullException`
- `AclInvalidRuleException`
- `TypeMismatchException`
- `ReadonlyException`
- `DictMissingException`
- `BigStringFileMissingException`
- `CannotReadException`

## What NOT To Do

### Avoid

- Empty JavaDoc blocks with no content
- `/** {@inheritDoc} */` - too verbose
- HTML tags (`<code>`, `<b>`, etc.) unless necessary
- Line lengths exceeding 120 characters
- JavaDoc on trivial private methods
- Documenting obvious behavior
- Repeating method signature in description

### Good

```java
/** WAL entry record for crash recovery. */
public record WalEntry(long seq, String key, byte[] value) { }
```

### Bad

```java
/**
 * This is a WalEntry record class.
 * It implements comparable interface.
 * Each entry has seq, key, and value.
 *
 * @author developer
 * @version 1.0
 */
public record WalEntry { }
```

## Formatting Rules

1. **4 spaces** for indentation (no tabs)
2. **120 characters** max line length
3. **Opening brace on same line** for class/method declarations
4. **Blank line** between summary and description (if description exists)

```java
/**
 * Summary line - single line preferred.
 *
 * Optional longer description when genuinely needed.
 */
public class Example {
    /**
     * @param slot the slot index (0-16383)
     * @return the slot data or null
     */
    public SlotData getSlot(int slot) { }
}
```

## Special Cases

### Records

JavaDoc on records should document the record's purpose and field constraints:

```java
/**
 * Represents a WAL entry with sequence number and key-value metadata.
 *
 * @param seq      the sequence number for ordering
 * @param key      the lookup key (non-null)
 * @param expireAt expiration timestamp in millis, null for no expiry
 */
public record WalEntry(long seq, String key, @Nullable Long expireAt) { }
```

### Enums

```java
/**
 * Replication state machine states.
 */
public enum ReplState {
    /** Connected to master and receiving updates. */
    SYNCED,
    /** Connection lost, attempting to reconnect. */
    RECONNECTING,
    /** Initial state before connection established. */
    IDLE
}
```

### Interfaces with Single Implementation

JavaDoc on interface methods is sufficient; implementation classes may omit if behavior is unchanged.

## Checklist

Before committing, verify:

- [ ] Class/interface has summary line
- [ ] Public constructors document all parameters
- [ ] Public methods document parameters and return
- [ ] Custom exceptions documented with `@throws`
- [ ] No unnecessary HTML or verbose descriptions
- [ ] Line length under 120 characters
- [ ] No empty JavaDoc blocks

## Examples

### Good Complete Class

```java
/**
 * LocalPersist manages local storage operations for a slot.
 * It handles WAL (Write-Ahead Log) reading and resource cleanup.
 */
public class LocalPersist {
    /** The slot index this instance manages. */
    private final short slot;

    /**
     * @param slot the slot index (0-16383)
     */
    public LocalPersist(short slot) {
        this.slot = slot;
    }

    /**
     * @param key the lookup key
     * @return the value or null if not found
     * @throws SegmentOverflowException when segment capacity exceeded
     */
    public byte[] get(String key) { }
}
```

### Minimal for Data Classes

```java
/** Configuration for a single slot's storage settings. */
public class ConfForSlot {
    /** Directory path for slot data files. */
    String dataDir;

    /** Maximum size in bytes for this slot. */
    long maxSize;

    // Getters/setters inherit from field docs
    public String getDataDir() { return dataDir; }
    public void setDataDir(String dir) { this.dataDir = dir; }
}
```
