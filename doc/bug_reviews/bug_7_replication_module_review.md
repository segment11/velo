# Replication Module Bug Review (Round 2)

Date: 2026-04-16

## Bug 1: XDict.decodeFrom() - Missing validation for negative dictBytesLength

**Severity:** Medium
**File:** `src/main/java/io/velo/repl/incremental/XDict.java:122-123`

```java
var dictBytesLength = buffer.getShort();
var dictBytes = new byte[dictBytesLength]; // NegativeArraySizeException if corrupted
```

`buffer.getShort()` can return a negative value from corrupted data. Unlike `keyPrefixLength` which is validated (line 115), `dictBytesLength` has **zero** validation. A corrupted binlog entry will throw `NegativeArraySizeException` with no useful context.

---

## Bug 2: XWalV.decodeFrom() - Missing validation for negative cvEncodedLength

**Severity:** Medium
**File:** `src/main/java/io/velo/repl/incremental/XWalV.java:138-139`

```java
var cvEncodedLength = buffer.getInt();
var cvEncoded = new byte[cvEncodedLength]; // NegativeArraySizeException if corrupted
```

Same pattern as Bug 1. `keyLength` is validated (line 132) but `cvEncodedLength` is not. This is the main data-carrying type, so corrupted network/binlog data could hit this path.

---

## Bug 3: XBigStrings.decodeFrom() - Missing validation for negative cvEncodedLength

**Severity:** Medium
**File:** `src/main/java/io/velo/repl/incremental/XBigStrings.java:159-160`

```java
var cvEncodedLength = buffer.getInt();
var cvEncoded = new byte[cvEncodedLength]; // NegativeArraySizeException if corrupted
```

Same pattern as Bug 2.

---

## Bug 4: XDynConfig - Completely unimplemented stub that will cause NPE

**Severity:** Critical (latent)
**File:** `src/main/java/io/velo/repl/incremental/XDynConfig.java:56-59`

```java
public static XDynConfig decodeFrom(ByteBuffer buffer) {
    return null; // placeholder
}
```

The type `dyn_config` (code 121) is registered in `BinlogContent.Type` and routed to `XDynConfig.decodeFrom()`. If a `dyn_config` binlog entry ever appears, `decodeFrom()` returns `null`, which gets added to the contents list in `Binlog.decode()` (line 752), and then `Binlog.decodeAndApply()` calls `null.apply(slot, replPair)` at line 763, causing a `NullPointerException`.

No code currently creates `XDynConfig` instances (`new XDynConfig` has zero hits), so this is a **latent** bug - it won't trigger today, but will crash if anyone adds dyn_config support on the encode side without implementing the decode side.

---

## Bug 5: XSkipApply.apply() - Extra closing brace in log format

**Severity:** Low
**File:** `src/main/java/io/velo/repl/incremental/XSkipApply.java:109`

```java
log.warn("Repl skip apply, seq={}}", seq);
```

The double `}}` produces output like `Repl skip apply, seq=123}` instead of `Repl skip apply, seq=123`.

---

## Bug 6: ToSlaveExistsBigString - Dead correction code for existCount

**Severity:** Low-Medium
**File:** `src/main/java/io/velo/repl/content/ToSlaveExistsBigString.java:110,125-127`

```java
existCount++;  // line 110 - unconditional, inside the for-each loop

// line 125-127 - this is always false
if (existCount != toSendIdList.size()) {
    ByteBuffer.wrap(toBuf.array()).putInt(countPosition, existCount);
}
```

`existCount` is incremented unconditionally on every iteration (line 110), so it always equals `toSendIdList.size()`. The correction logic at lines 125-127 is dead code. If a file fails to read (throws `RuntimeException` on line 121), the entire method fails, so the count-correction mechanism can never execute.

---

## Bug 7: XUpdateSeq.encodedLength() - Misleading comment

**Severity:** Trivial
**File:** `src/main/java/io/velo/repl/incremental/XUpdateSeq.java:71`

```java
// 4 bytes for time millis   <-- WRONG, it's actually 8 bytes (putLong)
return 1 + 8 + 8;
```

The comment says "4 bytes for time millis" but `timeMillis` is encoded as `putLong()` (8 bytes). The return value is correct but the comment is misleading.
