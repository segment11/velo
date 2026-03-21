# Velo Protocol Decoding Design

## Overview

Protocol decoding is implemented by
[`RequestDecoder`](/home/kerry/ws/velo/src/main/java/io/velo/decode/RequestDecoder.java).
It produces a list of [`Request`](/home/kerry/ws/velo/src/main/java/io/velo/decode/Request.java)
objects from ActiveJ `ByteBufs`.

The decoder recognizes three wire protocols:

- Redis RESP
- a minimal HTTP surface
- the internal `X-REPL` replication protocol

## Detection Order

The current code checks the first bytes of the incoming frame:

- HTTP if the request starts with `GET`, `POST`, `PUT`, or `DELETE`
- replication if it starts with `X-REPL`
- otherwise RESP

This is a pragmatic byte-prefix check, not a pluggable protocol registry.

## RESP Path

RESP parsing is delegated to [`RESP`](/home/kerry/ws/velo/src/main/java/io/velo/decode/RESP.java).
`RequestDecoder` keeps one RESP decoder instance per decoder object and repeatedly extracts complete
requests to support pipelining.

If the RESP decoder reports incomplete data, the outer decoder returns and waits for more bytes.

## HTTP Path

HTTP parsing uses [`HttpHeaderBody`](/home/kerry/ws/velo/src/main/java/io/velo/decode/HttpHeaderBody.java).

Current behavior is intentionally small:

- `GET` and `DELETE` read command tokens from the URL query string
- `POST` and `PUT` read a space-separated command body
- parsed headers are attached to the `Request`

This is sufficient for metrics and management-style endpoints, not a general REST framework.

## Replication Path

Replication decoding uses [`Repl.decode(...)`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Repl.java).
The frame structure is:

- `X-REPL`
- slave UUID
- slot
- repl type code
- content length
- encoded content bytes

The decoded payload becomes a [`ReplRequest`](/home/kerry/ws/velo/src/main/java/io/velo/repl/ReplRequest.java)
inside a `Request` marked as replication traffic.

## Request Object

[`Request`](/home/kerry/ws/velo/src/main/java/io/velo/decode/Request.java) is the common handoff object for all protocols.
It can carry:

- decoded command data (`byte[][]`)
- protocol flags (`isHttp`, `isRepl`)
- parsed HTTP headers
- big-string zero-copy metadata
- slot-routing metadata added later by `RequestHandler`

## Zero-Copy Support

Large RESP bulk strings can use
[`BigStringNoMemoryCopy`](/home/kerry/ws/velo/src/main/java/io/velo/decode/BigStringNoMemoryCopy.java).
`RequestDecoder` copies that state from the RESP parser into the resulting `Request`.

This optimization is specific to large string handling and does not apply to every request type.

## Related Documents

- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
- [Response Encoding](/home/kerry/ws/velo/doc/design/06_response_encoding_design.md)
- [Replication](/home/kerry/ws/velo/doc/design/09_replication_design.md)
