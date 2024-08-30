package io.velo.persist

import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorInputStream
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream

import java.nio.ByteBuffer

def blockSize = FramedLZ4CompressorOutputStream.BlockSize.K64
//def blockSize = FramedLZ4CompressorOutputStream.BlockSize.K256

def bos = new ByteArrayOutputStream((blockSize.size / 2).intValue())
def lz4 = new FramedLZ4CompressorOutputStream(bos, new FramedLZ4CompressorOutputStream.Parameters(blockSize))

// todo
int len = blockSize.size

byte[] data = new byte[len]
// get from file
def file = new File('/tmp/velo-data/persist/slot-0/worker-1-slot-0.data')
def fis = new FileInputStream(file)
println 'read ' + fis.read(data, 0, len)

println data.length
println 'buffer last 10 bytes: ' + data[-10..-1]

def begin = System.currentTimeMillis()
lz4.write(data, 0, len)
lz4.close()
def end = System.currentTimeMillis()
println 'compress time: ' + (end - begin) + ' ms'

def compressedBytes = bos.toByteArray()
println compressedBytes.length

println 'compress ratio: ' + (data.length / compressedBytes.length)


def lz4In = new FramedLZ4CompressorInputStream(new ByteArrayInputStream(compressedBytes))
def buf = ByteBuffer.allocate(len)
byte[] buffer = new byte[256]

begin = System.currentTimeMillis()
int n
while(-1 != (n = lz4In.read(buffer))) {
    // do nothing
    buf.put(buffer, 0, n)
}
end = System.currentTimeMillis()
println 'decompress time: ' + (end - begin) + ' ms'

println 'buf length: ' + buf.limit()

byte[] dst = new byte[10]
buf.position(buf.limit() - 10).get(dst)
println 'buf last 10 bytes: ' + dst