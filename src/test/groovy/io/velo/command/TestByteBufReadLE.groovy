package io.velo.command

import io.netty.buffer.Unpooled

byte[] bytes = [2, 0, 0, 0]
def buf = Unpooled.wrappedBuffer(bytes)

buf.markReaderIndex()
println buf.readInt()
buf.resetReaderIndex()

println buf.readIntLE()