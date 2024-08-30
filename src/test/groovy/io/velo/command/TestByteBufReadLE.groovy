package io.velo.command

import io.netty.buffer.Unpooled

byte[] bytes = [2, 0, 0, 0]
def bb = Unpooled.wrappedBuffer(bytes)

bb.markReaderIndex()
println bb.readInt()
bb.resetReaderIndex()

println bb.readIntLE()