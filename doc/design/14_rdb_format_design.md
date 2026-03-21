# Velo RDB Format Design

## Overview

RDB support is implemented around
[`RDBParser`](/home/kerry/ws/velo/src/main/java/io/velo/rdb/RDBParser.java)
and import helpers under [`io/velo/command`](/home/kerry/ws/velo/src/main/java/io/velo/command).

The current implementation is focused on parsing and importing Redis RDB data, not on being a complete
drop-in replacement for every Redis RDB feature.

## Version Support

`RDBParser` currently defines:

- minimum version `6`
- target/current version `11`

## Supported Value Families

The parser contains handlers for:

- string
- list
- set
- zset
- hash

It also supports several Redis compact encodings, including:

- ziplist-backed forms
- listpack-backed forms
- intset
- quicklist
- LZF-compressed strings

## Unsupported Areas

The parser explicitly rejects stream encodings with an exception in the current code.
Design docs should therefore not claim stream import support.

## Import Path

Import-related classes include:

- [`RDBImporter`](/home/kerry/ws/velo/src/main/java/io/velo/command/RDBImporter.java)
- [`VeloRDBImporter`](/home/kerry/ws/velo/src/main/java/io/velo/command/VeloRDBImporter.java)
- [`MyRDBVisitorEventListener`](/home/kerry/ws/velo/src/main/java/io/velo/command/MyRDBVisitorEventListener.java)

These classes bridge parsed RDB entries into the normal Velo slot and command infrastructure.

## Related Documents

- [Type System](/home/kerry/ws/velo/doc/design/03_type_system_design.md)
- [Command Processing](/home/kerry/ws/velo/doc/design/04_command_processing_design.md)
