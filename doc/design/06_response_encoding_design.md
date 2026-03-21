# Velo Response Encoding Design

## Overview

Response encoding is centered on the
[`Reply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/Reply.java) interface and its concrete implementations
under [`io/velo/reply`](/home/kerry/ws/velo/src/main/java/io/velo/reply).

The interface exposes three output forms:

- `buffer()` for the default RESP representation
- `bufferAsResp3()` for RESP3-aware replies
- `bufferAsHttp()` for replies that can be rendered directly as HTTP bodies

## Reply Hierarchy

Important reply types include:

- [`OKReply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/OKReply.java)
- [`ErrorReply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/ErrorReply.java)
- [`IntegerReply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/IntegerReply.java)
- [`BulkReply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/BulkReply.java)
- [`MultiBulkReply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/MultiBulkReply.java)
- [`NilReply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/NilReply.java)
- [`AsyncReply`](/home/kerry/ws/velo/src/main/java/io/velo/reply/AsyncReply.java)

## RESP2 And RESP3

Most replies are RESP2-first. RESP3 support is incremental: a reply can override `bufferAsResp3()`
when it needs a different on-wire representation.

The important design point is that protocol selection happens after command execution. Commands return
typed replies and the network layer chooses which encoder view to use.

## HTTP Output

HTTP output is intentionally narrow.

- Some replies implement `bufferAsHttp()` directly.
- [`MultiWorkerServer`](/home/kerry/ws/velo/src/main/java/io/velo/MultiWorkerServer.java) wraps those bodies
  with HTTP status headers using constants from
  [`HttpHeaderBody`](/home/kerry/ws/velo/src/main/java/io/velo/decode/HttpHeaderBody.java).
- Nil and error cases are mapped to `404` and `500` style responses in the server layer.

This means HTTP encoding is a server concern plus per-reply body support, not a separate reply stack.

## Replication Replies

Replication does not use the normal RESP reply classes. It uses:

- [`Repl.ReplReply`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Repl.java)
- [`Repl.ReplReplyFromBytes`](/home/kerry/ws/velo/src/main/java/io/velo/repl/Repl.java)

Those records also implement `Reply`, which lets the outer server pipeline treat replication output
uniformly.

## Corrections To Older Versions

- The current file should not claim a complete standalone RESP3 encoder layer. RESP3 is exposed via optional overrides on reply objects.
- HTTP encoding appears only once in the design. Earlier duplicated section headings were documentation drift.

## Related Documents

- [Protocol Decoding](/home/kerry/ws/velo/doc/design/05_protocol_decoding_design.md)
- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
