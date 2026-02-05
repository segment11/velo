# Velo Response Encoding Design

## Table of Contents
- [Overview](#overview)
- [Reply Type Hierarchy](#reply-type-hierarchy)
- [RESP2 Encoding](#resp2-encoding)
- [RESP3 Encoding](#resp3-encoding)
- [HTTP Encoding](#http-encoding)
- [Reply Type Implementations](#reply-type-implementations)
- [Performance Optimizations](#performance-optimizations)
- [Error Response Format](#error-response-format)
- [Related Documentation](#related-documentation)

---

## Overview

Velo's response system converts Java objects into **wire protocol representations** for transmission to clients. The system supports three protocols:

1. **RESP2** - Redis Serialization Protocol version 2 (default)
2. **RESP3** - Enhanced Redis protocol with new types
3. **HTTP** - JSON and text-based responses

### Response Characteristics

| Protocol | Default | Primary Use | Transport |
|----------|---------|--------------|-----------|
| RESP2 | Yes | Redis clients | TCP |
| RESP3 | Optional | Modern clients (Redis 6+) | TCP |
| HTTP | No | REST integration | TCP (HTTP/1.1) |

### Design Goals

- **Protocol compatibility**: Support RESP2 and RESP3 clients interchangeably
- **Performance**: Cached buffers for common values, minimal allocations
- **Binary safety**: Handle arbitrary binary data correctly
- **Error clarity**: meaningful error messages in protocol format

---

## Reply Type Hierarchy

### Interface

```java
public interface Reply {
    // RESP2 encoding
    ByteBuf buffer();

    // RESP3 encoding (default falls back to RESP2)
    default ByteBuf bufferAsResp3() {
        return buffer(); // RESP3 not supported, use RESP2
    }

    // HTTP encoding (null if not HTTP)
    default ByteBuf bufferAsHttp() {
        return null; // Not HTTP-compatible
    }

    // Reply content (for programmatic access)
    default Object getContent() {
        return null;
    }
}
```

### Reply Types

```
Reply (interface)
├── OKReply                  # Simple "OK" response
├── ErrorReply               # Error message
├── IntegerReply             # Numeric response
├── NilReply                 # Null/empty response
├── BulkReply                # Binary-safe string
├── MultiBulkReply           # Array of replies
├── BoolReply                # Boolean (RESP3)
├── DoubleReply              # Floating-point
├── PongReply                # PING response
├── EmptyReply               # Empty response
└── AsyncReply               # Async operation placeholder
```

### Reply Type Summary

| Type | RESP2 Marker | RESP3 Marker | Usage |
|------|--------------|--------------|-------|
| OKReply | `+OK\r\n` | `+OK\r\n` | Simple success (SET, DEL, etc.) |
| ErrorReply | `-ERR msg\r\n` | -ERR msg\r\n` | Errors |
| IntegerReply | `:123\r\n` | `:123\r\n` | Counts, indices |
| BulkReply | `$length\r\ndata\r\n` | Depends on data | Key values |
| MultiBulkReply | `*count\r\nelements\r\n` | `*count\r\n...` | Arrays (MGET, LRANGE) |
| NilReply | `$-1\r\n` | `_\r\n` | Not found |
| BoolReply | `:1\r\n`:0\r\n` | `#t\r\n`:`#f\r\n` | Boolean (RESP3) |
| DoubleReply | String rep | Number rep | Floating-point |
| PongReply | `+PONG\r\n` | `+PONG\r\n` | PING response |
| EmptyReply | Array length 0 | Array length 0 | No results |

---

## RESP2 Encoding

### Encoding Rules

**Simple Strings (OKReply, PongReply):**
```
Format: +<string>\r\n
Example: +OK\r\n
         +PONG\r\n

Rules:
  - Cannot contain \r or \n
  - Limited to printable ASCII
  - Most common for success responses
```

**Errors (ErrorReply):**
```
Format: -<error-message>\r\n
Example: -ERR wrong number of arguments\r\n
         -WRONGTYPE Operation against a key holding the wrong kind of value\r\n

Rules:
  - First word is error type (ERR, WRONGTYPE, NOAUTH, etc.)
  - Space, then message
  - Similar to simple strings but indicates error
```

**Integers (IntegerReply):**
```
Format: :<number>\r\n
Example: :123\r\n
         :0\r\n
        :-42\r\n

Rules:
  - Signed 64-bit integers
  - No decimal point
  - Uses decimal string representation
```

**Bulk Strings (BulkReply):**
```
Format: $<length>\r\n<data>\r\n
Example: $6\r\nfoobar\r\n   (6 bytes: "foobar")
         $0\r\n\r\n          (empty string)
        $-1\r\n              (null/nil)

Rules:
  - Binary-safe: can contain any bytes including \0
  - Length is number of bytes (not characters)
  - $-1 represents null
  - $0 is empty string
```

**Arrays (MultiBulkReply):**
```
Format: *<count>\r\n<element-1><element-2>...<element-n>\r\n
Example: *2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n
         *0\r\n                   (empty array)
        *-1\r\n                  (null array)

Nested Arrays:
  *2\r\n*2\r\n$3\r\nfoo\r\n$3\r\nbar\r\n$3\r\nbaz\r\n
  → [["foo","bar"], "baz"]

Rules:
  - Count is number of elements (can be any reply type)
  - Null arrays: *-1
  - Empty arrays: *0
```

### Example Encodings

**Single Key Get:**
```
Command: GET key
Reply: $5\r\nvalue\r\n

Breakdown:
  - Type marker: $ (bulk string)
  - Length: 5
  - Separator: \r\n
  - Data: "value"
  - Terminator: \r\n
```

**Multiple Key Get:**
```
Command: MGET key1 key2
Reply: *2\r\n$4\r\nval1\r\n$4\r\nval2\r\n

Breakdown:
  - Type marker: * (array)
  - Count: 2
  - Separator: \r\n
  - Element 1: $4\r\nval1\r\n
  - Element 2: $4\r\nval2\r\n
```

**Integer Count:**
```
Command: DBSIZE
Reply: :1024\r\n

Breakdown:
  - Type marker: : (integer)
  - Value: 1024
  - Terminator: \r\n
```

**Simple Success:**
```
Command: SET key value
Reply: +OK\r\n

Breakdown:
  - Type marker: + (simple string)
  - Message: OK
  - Terminator: \r\n
```

---

## RESP3 Encoding

### RESP3 Enhancements

RESP3 adds new types and semantics:

**New Markers:**

| Marker | Type | Description |
|--------|------|-------------|
| `>` | Push messages | Server-initiated updates |
| `~` | Set | Distinct string collection |
| `%` | Map | Key-value pairs |
| `|` | Attribute | Optional type attributes |
| `_` | Null | Lightweight null |
| `#` | Boolean | True/False |
| `,` | Double | Floating-point number |

### RESP3 vs RESP2 Differences

**Null Representation:**
```
RESP2: $-1\r\n   (5 bytes)
RESP3: _\r\n      (2 bytes, more compact)
```

**Boolean:**
```
RESP2: :1\r\n     (integer true)
       :0\r\n     (integer false)
RESP3: #t\r\n    (explicit boolean)
       #f\r\n
```

**Double:**
```
RESP2: $12\r\n3.1415926535\r\n   (string representation)
RESP3: ,3.1415926535\r\n        (explicit double)
```

**Set:**
```
RESP2: *3\r\n$3\r\na\r\n$3\r\nb\r\n$3\r\nc\r\n   (array)
RESP3: ~3\r\n$3\r\na\r\n$3\r\nb\r\n$3\r\nc\r\n    (set)
```

**Map:**
```
RESP2: *4\r\n$3\r\na\r\n$1\r\n1\r\n$3\r\nb\r\n$1\r\n2\r\n
       (array of key-value pairs)
RESP3: %2\r\n$3\r\na\r\n:1\r\n$3\r\nb\r\n:2\r\n (explicit map)
```

### RESP3 Implementation

```java
public class BulkReply extends Reply {
    public ByteBuf buffer() {
        // RESP2: Bulk string
        return encodeResp2();
    }

    public ByteBuf bufferAsResp3() {
        // Check if qualifies as simple string
        if (isSimpleString()) {
            // RESP3 simple string: shorter format
            return encodeSimpleString();
        }

        // RESP3 bulk string (same as RESP2)
        return encodeResp2();
    }

    private boolean isSimpleString() {
        // < 32 bytes, valid ASCII, no special chars
        if (data.length > 32) {
            return false;
        }

        for (byte b : data) {
            if (b < 32 || b >= 127) {
                return false; // Non-printable or extended ASCII
            }
        }

        return true;
    }

    private ByteBuf encodeSimpleString() {
        // "#_<string>\r\n" format for RESP3
        ByteBuf buf = Unpooled.buffer(data.length + 4);
        buf.writeByte('#');
        buf.writeBytes(data);
        buf.writeByte('\r');
        buf.writeByte('\n');
        return buf;
    }
}

public class BoolReply extends Reply {
    public static final BoolReply TRUE = new BoolReply(true);
    public static final BoolReply FALSE = new BoolReply(false);

    public ByteBuf buffer() {
        // RESP2: Use integers
        return Unpooled.copiedBuffer(":" + (value ? "1" : "0") + "\r\n",
                                        StandardCharsets.US_ASCII);
    }

    public ByteBuf bufferAsResp3() {
        // RESP3: Explicit boolean type
        String s = value ? "t" : "f";
        return Unpooled.copiedBuffer("#" + s + "\r\n",
                                        StandardCharsets.US_ASCII);
    }
}
```

---

## HTTP Encoding

## HTTP Encoding

### HTTP Response Format

```
HTTP/1.1 {status} {statusText}\r\n
Cache-Control: no-cache, no-store\r\n
Content-Type: text/plain\r\n
Content-Length: {length}\r\n
\r\n
{body as string/integer/bool}
```

### HTTP Response Types

```
String Value (BulkReply):
  HTTP/1.1 200 OK\r\n
  Cache-Control: no-cache, no-store\r\n
  Content-Type: text/plain\r\n
  Content-Length: 5\r\n
  \r\n
  value

Integer Value (IntegerReply):
  HTTP/1.1 200 OK\r\n
  Cache-Control: no-cache, no-store\r\n
  Content-Type: text/plain\r\n
  Content-Length: 2\r\n
  \r\n
  42

Simple Success (OKReply):
  HTTP/1.1 200 OK\r\n
  Cache-Control: no-cache, no-store\r\n
  Content-Type: text/plain\r\n
  Content-Length: 2\r\n
  \r\n
  OK

Null Value (NilReply):
  HTTP/1.1 200 OK\r\n
  Cache-Control: no-cache, no-store\r\n
  Content-Type: text/plain\r\n
  Content-Length: 2\r\n
  \r\n
  nil

Empty Array (MultiBulkReply.EMPTY):
  HTTP/1.1 200 OK\r\n
  Cache-Control: no-cache, no-store\r\n
  Content-Type: text/plain\r\n
  Content-Length: 13\r\n
  \r\n
  (empty array)

Error Response (ErrorReply):
  HTTP/1.1 500 Internal Server Error\r\n
  Cache-Control: no-cache, no-store\r\n
  Content-Type: text/plain\r\n
  Content-Length: {error length}\r\n
  \r\n
  {error message}
```

### Implementation Details

```java
// From MultiWorkerServer.java - HTTP response construction
ByteBuf assembleHttpResponse(Reply reply, boolean isError, boolean isNil) {
    byte[] body = reply.bufferAsHttp().array();

    String status;
    if (isError) {
        status = "500 Internal Server Error";
    } else if (isNil) {
        status = "404 Not Found";
    } else {
        status = "200 OK";
    }

    // Handle special 401 Unauthorized
    if (isError && (reply == ErrorReply.NO_AUTH || reply == ErrorReply.AUTH_FAILED || reply == ErrorReply.ACL_PERMIT_LIMIT)) {
        return ByteBuf.wrapForReading(HttpHeaderBody.HEADER_401);
    }

    byte[] contentLengthBytes = String.valueOf(body.length).getBytes();

    String headerPrefix = isError ? "HTTP/1.1 500 Internal Server Error\r\n" :
                           isNil ? "HTTP/1.1 404 Not Found\r\n" :
                           "HTTP/1.1 200 OK\r\n";

    headerPrefix += "Cache-Control: no-cache, no-store\r\n";
    headerPrefix += "Content-Type: text/plain\r\n";
    headerPrefix += "Content-Length: " + body.length + "\r\n";
    headerPrefix += "\r\n";

    return headerPrefix.getBytes() + contentLengthBytes +
           HttpHeaderBody.HEADER_SUFFIX +
           body;
}

// Example from MultiBulkReply (empty array)
private static final byte[] EMPTY_HTTP_BODY_BYTES = "(empty array)".getBytes();

// Example from ErrorReply
private static final String message;  // e.g., "auth failed !WRONGPASS!"
public ByteBuf bufferAsHttp() {
    return ByteBuf.wrapForReading(message.getBytes());
}

// Example from IntegerReply (value 1)
private static final byte b1 = (byte)1';
public ByteBuf bufferAsHttp() {
    return ByteBuf.wrapForReading(b1);
}

// Example from OKReply
private static final byte[] HTTP_BODY_BYTES = "OK".getBytes();
```

### Key Characteristics

- **Content-Type: text/plain** - Not JSON
- **No JSON wrapper** - Raw Reply content converted to string
- **Body content format**:
  - String values: The raw byte data converted to string (potentially with escape sequences)
  - Integer values: The numeric value as decimal string
  - Boolean values: "true" or "false"
  - Arrays: "(empty array)" or JSON-like string representation
  - Errors: Just the error message text
  - Null: "nil"
  - Success: "OK"
- **Headers**: Cache-Control, Content-Type, Content-Length
- **No envelope**: Direct string/integer/bool, no JSON wrapper

### Comparison with Documented Version

**Wrong (as in current document):**
- JSON format with `{"value": "..."}`
- JSON arrays for results
- HTTP status codes for errors

**Correct (actual implementation):**
- Plain text/plain responses
- Raw values as strings/integers/booleans
- Simple text for errors
- No JSON structure
- HTTP headers only for protocol compliance

```
    private static ByteBuf encodeHttpResponse(int statusCode, String json) {
        String http = String.format(
            "HTTP/1.1 %d OK\r\n" +
            "Content-Type: application/json\r\n" +
            "Content-Length: %d\r\n" +
            "\r\n" +
            "%s",
            statusCode,
            json.getBytes(StandardCharsets.UTF_8).length,
            json
        );

        return Unpooled.copiedBuffer(http, StandardCharsets.UTF_8);
    }
}
```

---

## Reply Type Implementations

### OKReply

```java
public final class OKReply extends Reply {
    public static final OKReply INSTANCE = new OKReply();

    private OKReply() {}

    public ByteBuf buffer() {
        return Unpooled.copiedBuffer("+OK\r\n", StandardCharsets.US_ASCII);
    }

    public ByteBuf bufferAsHttp() {
        return HttpReplyEncoder.encodeOkAsJson();
    }
}
```

### ErrorReply

```java
public final class ErrorReply extends Reply {
    public static final ErrorReply FORMAT =
        new ErrorReply("ERR wrong number of arguments",
                        "wrong number of arguments");

    public static final ErrorReply SYNTAX =
        new ErrorReply("ERR syntax error", "syntax error");

    public static final ErrorReply WRONG_TYPE =
        new ErrorReply("WRONGTYPE Operation against a key holding the wrong kind of value",
                        "Operation against a key holding the wrong kind of value");

    public static final ErrorReply KEY_NOT_FOUND =
        new ErrorReply("NIL value not found", "value not found");

    public static final ErrorReply NOT_INTEGER =
        new ErrorReply("ERR value is not an integer or out of range",
                        "value is not an integer or out of range");

    public static final ErrorReply NO_SUCH_KEY =
        new ErrorReply("ERR no such key", "no such key");

    private final String type;       // Error type ("ERR", "WRONGTYPE", etc.)
    private final String message;    // Error message

    public ErrorReply(String fullMessage, String message) {
        this.fullMessage = fullMessage;
        this.message = message;
    }

    public ByteBuf buffer() {
        return Unpooled.copiedBuffer("-" + fullMessage + "\r\n",
                                        StandardCharsets.US_ASCII);
    }

    public ByteBuf bufferAsHttp() {
        return HttpReplyEncoder.encodeErrorAsJson(this);
    }
}
```

### IntegerReply

```java
public final class IntegerReply extends Reply {
    private final long value;

    private static final LongMap<ByteBuf> COMMON_VALUES = new LongMap<>();

    // Cache common values (-10 to 100)
    static {
        for (long i = -10; i <= 100; i++) {
            COMMON_VALUES.put(i, Unpooled.wrappedBuffer(
                (":" + i + "\r\n").getBytes(StandardCharsets.US_ASCII)
            ));
        }
    }

    public IntegerReply(long value) {
        this.value = value;
    }

    public static IntegerReply of(long value) {
        if (value >= -10 && value <= 100) {
            // Return cached, immutable instance
            return new CachedIntegerReply(value);
        }
        return new IntegerReply(value);
    }

    public ByteBuf buffer() {
        // Check cache for common values
        ByteBuf cached = COMMON_VALUES.get(value);
        if (cached != null) {
            return cached.retainedSlice();
        }

        // Create new buffer
        return Unpooled.copiedBuffer(":" + value + "\r\n",
                                        StandardCharsets.US_ASCII);
    }

    public ByteBuf bufferAsHttp() {
        return HttpReplyEncoder.encodeIntegerAsJson(this);
    }
}
```

### BulkReply

```java
public final class BulkReply extends Reply {
    private byte[] data;

    public BulkReply(byte[] data) {
        this.data = data;
    }

    public static BulkReply bulkReply(byte[] data) {
        if (data == null) {
            return NilReply.INSTANCE;
        }
        return new BulkReply(data);
    }

    public static BulkReply bulkReply(String s) {
        return bulkReply(s.getBytes(StandardCharsets.UTF_8));
    }

    public ByteBuf buffer() {
        if (data == null) {
            return NilReply.INSTANCE.buffer();
        }

        ByteBuf buf = Unpooled.buffer(data.length + 16);
        buf.writeByte('$');
        writeAscii(buf, data.length);
        buf.writeByte('\r');
        buf.writeByte('\n');
        buf.writeBytes(data);
        buf.writeByte('\r');
        buf.writeByte('\n');
        return buf;
    }

    public ByteBuf bufferAsResp3() {
        if (isSimpleString()) {
            // RESP3 simple string
            ByteBuf buf = Unpooled.buffer(data.length + 4);
            buf.writeByte('+');
            buf.writeBytes(data);
            buf.writeByte('\r');
            buf.writeByte('\n');
            return buf;
        }
        return buffer(); // Same as RESP2
    }

    public ByteBuf bufferAsHttp() {
        return HttpReplyEncoder.encodeBulkAsJson(this);
    }

    private void writeAscii(ByteBuf buf, long value) {
        // Write decimal ASCII representation
        byte[] ascii = Long.toString(value).getBytes(StandardCharsets.US_ASCII);
        buf.writeBytes(ascii);
    }
}
```

### MultiBulkReply

```java
public final class MultiBulkReply extends Reply {
    public final Reply[] replies;

    public MultiBulkReply(Reply[] replies) {
        this.replies = replies;
    }

    public static MultiBulkReply multiBulkReply(Reply[] replies) {
        if (replies == null || replies.length == 0) {
            return EMPTY;
        }
        return new MultiBulkReply(replies);
    }

    public static final MultiBulkReply EMPTY =
        new MultiBulkReply(new Reply[0]);

    public ByteBuf buffer() {
        ByteBuf buf = Unpooled.buffer();

        // Array marker
        buf.writeByte('*');
        writeAscii(buf, replies.length);
        buf.writeByte('\r');
        buf.writeByte('\n');

        // Each element
        for (Reply reply : replies) {
            if (reply == null || reply == NilReply.INSTANCE) {
                buf.writeBytes(NilReply.INSTANCE.buffer());
            } else {
                buf.writeBytes(reply.buffer());
            }
        }

        return buf;
    }

    public ByteBuf bufferAsResp3() {
        // Same protocol for arrays in RESP2 and RESP3
        return buffer();
    }

    public ByteBuf bufferAsHttp() {
        return HttpReplyEncoder.encodeMultiBulkAsJson(this);
    }
}
```

### NilReply

```java
public final class NilReply extends Reply {
    public static final NilReply INSTANCE = new NilReply();

    private NilReply() {}

    public ByteBuf buffer() {
        // RESP2: $-1\r\n
        return Unpooled.copiedBuffer("$-1\r\n", StandardCharsets.US_ASCII);
    }

    public ByteBuf bufferAsResp3() {
        // RESP3: _\r\n (lighter weight)
        return Unpooled.copiedBuffer("_\r\n", StandardCharsets.US_ASCII);
    }

    public ByteBuf bufferAsHttp() {
        return HttpReplyEncoder.encodeNullAsJson();
    }
}
```

### BoolReply

```java
public final class BoolReply extends Reply {
    public static final BoolReply TRUE = new BoolReply(true);
    public static final BoolReply FALSE = new BoolReply(false);

    private final boolean value;

    private BoolReply(boolean value) {
        this.value = value;
    }

    public ByteBuf buffer() {
        // RESP2: Use integers
        return value ?
               IntegerReply.of(1).buffer() :
               IntegerReply.of(0).buffer();
    }

    public ByteBuf bufferAsResp3() {
        // RESP3: Explicit boolean (#t or #f)
        return value ?
               Unpooled.copiedBuffer("#t\r\n", StandardCharsets.US_ASCII) :
               Unpooled.copiedBuffer("#f\r\n", StandardCharsets.US_ASCII);
    }
}
```

### DoubleReply

```java
public final class DoubleReply extends Reply {
    private final double value;

    public DoubleReply(double value) {
        this.value = value;
    }

    public ByteBuf buffer() {
        // RESP2: String representation
        return BulkReply.bulkReply(Double.toString(value)).buffer();
    }

    public ByteBuf bufferAsResp3() {
        // RESP3: Explicit double (,)
        return Unpooled.copiedBuffer("," + value + "\r\n",
                                        StandardCharsets.US_ASCII);
    }
}
```

### PongReply

```java
public final class PongReply extends Reply {
    public static final PongReply INSTANCE = new PongReply();

    private PongReply() {}

    public ByteBuf buffer() {
        return Unpooled.copiedBuffer("+PONG\r\n", StandardCharsets.US_ASCII);
    }
}
```

### AsyncReply

```java
public class AsyncReply extends Reply {
    private final Promise<Reply> promise;

    public AsyncReply(Promise<Reply> promise) {
        this.promise = promise;
    }

    public Reply getSync() {
        // Blocking wait for result
        return promise.getSync();
    }

    public ByteBuf buffer() {
        // Can't serialize async reply directly
        throw new IllegalStateException(
            "AsyncReply cannot be serialized directly. Call getSync() first."
        );
    }
}
```

---

## Performance Optimizations

### Cached Common Values

**Integer -10 to 100:**
```java
private static final LongMap<ByteBuf> COMMON_VALUES = new LongMap<>();

static {
    for (long i = -10; i <= 100; i++) {
        COMMON_VALUES.put(i, Unpooled.wrappedBuffer(
            (":" + i + "\r\n").getBytes(StandardCharsets.ISO_8859_1)
        ));
    }
}
```

**Bulk Empty String:**
```java
private static final ByteBuf BULK_EMPTY =
    Unpooled.wrappedBuffer("$0\r\n\r\n".getBytes(StandardCharsets.ISO_8859_1));
```

**Nil Marker:**
```java
private static final ByteBuf NIL_BYTES =
    Unpooled.wrappedBuffer("$-1\r\n".getBytes(StandardCharsets.ISO_8859_1));

private static final ByteBuf NIL_RESP3_BYTES =
    Unpooled.wrappedBuffer("_\r\n".getBytes(StandardCharsets.ISO_8859_1));
```

### Reusable Buffers

```java
class ReplyBufferPool {
    private static final Queue<ByteBuf> pool = new ConcurrentLinkedQueue<>();

    public static ByteBuf allocate(int size) {
        ByteBuf buf = pool.poll();
        if (buf != null && buf.capacity() >= size) {
            buf.clear();
            return buf;
        }
        return Unpooled.buffer(size);
    }

    public static void release(ByteBuf buf) {
        if (buf.capacity() <= 1024) { // Only pool small buffers
            buf.retain();
            pool.offer(buf);
        }
    }
}
```

### Direct Byte Access

```java
public ByteBuf buffer() {
    // Use slice() to share underlying array without copying
    ByteBuf buf = Unpooled.wrappedBuffer(data);
    return buf.slice(); // Read-only view
}
```

### ASCII Encoding Optimization

```java
private void writeAscii(LongString str, long number) {
    // Avoid UTF-8 encoder for numbers
    byte[] ascii = Long.toString(number).getBytes(StandardCharsets.US_ASCII);
    if (buf.capacity() < pos + ascii.length) {
        buf.capacity(pos + ascii.length);
    }
    System.arraycopy(ascii, 0, buf.array(), pos, ascii.length);
    pos += ascii.length;
}
```

---

## Error Response Format

### Error Message Convention

```
Format: <TYPE> <message>

Common Types:
  ERR           - General errors
  WRONGTYPE     - Type mismatch
  NOAUTH        - Authentication required
  NOPERM        - Permission denied
  NOSCRIPT      - Script not found
  MOVING        - Key is being migrated
  ERR syntax    - Syntax error

Examples:
  -ERR wrong number of arguments for 'set' command
  -WRONGTYPE Operation against a key holding the wrong kind of value
  -NOAUTH Authentication required
  -NOPERM this user has no permissions to run the 'get' command
  -NOSCRIPT No matching script. Please use EVAL.
```

### Error Response Examples

**Argument Error:**
```
Client: SET key
Server: -ERR wrong number of arguments for 'set' command
```

**Type Mismatch:**
```
Client: LPOP str_key
Server: -WRONGTYPE Operation against a key holding the wrong kind of value
```

**Authentication:**
```
Client: GET auth_key
Server: -NOAUTH Authentication required.
```

**Permission:**
```
Client: DEL admin_key
Server: -NOPERM this user has no permissions to run the 'del' command
```

**Key Not Found:**
```
Client: GET nonexistent_key
Server: $-1 (nil response)
```

---

## Related Documentation

### Design Documents
- [Overall Architecture](./01_overall_architecture.md) - System overview
- [Protocol Decoding Design](./05_protocol_decoding_design.md) - Request parsing
- [Command Processing Design](./04_command_processing_design.md) - Command to reply flow

### Key Source Files
**Reply Implementations:**
- `src/main/java/io/velo/reply/Reply.java` - Base interface
- `src/main/java/io/velo/reply/OKReply.java`
- `src/main/java/io/velo/reply/ErrorReply.java`
- `src/main/java/io/velo/reply/IntegerReply.java`
- `src/main/java/io/velo/reply/BulkReply.java`
- `src/main/java/io/velo/reply/MultiBulkReply.java`
- `src/main/java/io/velo/reply/NilReply.java`
- `src/main/java/io/velo/reply/BoolReply.java`
- `src/main/java/io/velo/reply/DoubleReply.java`
- `src/main/java/io/velo/reply/PongReply.java`
- `src/main/java/io/velo/reply/EmptyReply.java`
- `src/main/java/io/velo/reply/AsyncReply.java`

---

**Document Version:** 1.0
**Last Updated:** 2025-02-05
**Author:** Velo Architecture Team
