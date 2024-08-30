package io.velo.persist


import net.jpountz.lz4.LZ4Factory
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream

//def blockSize = FramedLZ4CompressorOutputStream.BlockSize.K64
def blockSize = FramedLZ4CompressorOutputStream.BlockSize.K256

def bos = new ByteArrayOutputStream((blockSize.size / 2).intValue())
def lz4 = LZ4Factory.nativeInstance().fastCompressor()
def lz4Decompressor = LZ4Factory.nativeInstance().fastDecompressor()

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
def compressedBytes = lz4.compress(data)
def end = System.currentTimeMillis()
println 'compress time: ' + (end - begin) + ' ms'

println compressedBytes.length

println 'compress ratio: ' + (data.length / compressedBytes.length)

begin = System.currentTimeMillis()
def uncompressed = lz4Decompressor.decompress(compressedBytes, len)
end = System.currentTimeMillis()

end = System.currentTimeMillis()
println 'decompress time: ' + (end - begin) + ' ms'

println 'uncompressed data length: ' + uncompressed.length
println 'uncompressed data last 10 bytes: ' + uncompressed[-10..-1]