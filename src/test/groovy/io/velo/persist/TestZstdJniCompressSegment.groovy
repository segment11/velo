package io.velo.persist

import com.github.luben.zstd.Zstd
import io.velo.TrainSampleJob
import io.velo.TrainSampleJob.TrainSampleKV
import org.apache.commons.compress.compressors.lz4.FramedLZ4CompressorOutputStream

def blockSize = FramedLZ4CompressorOutputStream.BlockSize.K64
//def blockSize = FramedLZ4CompressorOutputStream.BlockSize.K256
//def blockSize = FramedLZ4CompressorOutputStream.BlockSize.M1

// todo
int len = blockSize.size
int compressLevel = 3

byte[] data = new byte[len]
// get from file
def file = new File('/tmp/velo-data/persist/slot-0/worker-1-slot-0.data')
def fis = new FileInputStream(file)
println 'read ' + fis.read(data, 0, len)

println data.length
println 'data last 10 bytes: ' + data[-10..-1]

// train sample for each page size
int pageSize = 4096
def keyPrefix = 'seg__'

byte[] bytesFirstPage = data[0..<pageSize]
println 'first page last 10 bytes: ' + bytesFirstPage[-10..-1]

def trainSampleJob = new TrainSampleJob((byte) 0)
trainSampleJob.setDictSize(pageSize)

List<TrainSampleKV> list = []
int pages = len / pageSize
pages.times {
    byte[] bytesThisPage = data[it * pageSize..<(it + 1) * pageSize]
    list << new TrainSampleKV(keyPrefix + it, (long) it, bytesThisPage)
}
trainSampleJob.resetSampleToTrainList(list)

def begin = System.currentTimeMillis()
def trainSampleResult = trainSampleJob.train()
def end = System.currentTimeMillis()
def dict = trainSampleResult.cacheDict().get(keyPrefix)
println 'train dict time: ' + (end - begin) + ' ms'
println 'dict size: ' + dict.getDictBytes().length

begin = System.currentTimeMillis()
def compressedBytes = Zstd.compressUsingDict(data, dict.dictBytes, compressLevel)
end = System.currentTimeMillis()
println 'compress time use dict: ' + (end - begin) + ' ms'
println 'compressed data length: ' + compressedBytes.length
println 'compress ratio: ' + (data.length / compressedBytes.length)
println '--- --- --- --- --- --- --- --- --- --- --- --- '

begin = System.currentTimeMillis()
def compressedBytesNoDict = Zstd.compress(data, compressLevel)
end = System.currentTimeMillis()
println 'compress time no dict: ' + (end - begin) + ' ms'
println 'compressed data length: ' + compressedBytesNoDict.length
println 'compress ratio: ' + (data.length / compressedBytesNoDict.length)
println '--- --- --- --- --- --- --- --- --- --- --- --- '

begin = System.currentTimeMillis()
def compressedBytesFirstPage = Zstd.compressUsingDict(bytesFirstPage, dict.dictBytes, compressLevel)
end = System.currentTimeMillis()
println 'compress first page time use dict: ' + (end - begin) + ' ms'
println 'compressed data length: ' + compressedBytesFirstPage.length
println 'compress ratio: ' + (pageSize / compressedBytesFirstPage.length)
println '--- --- --- --- --- --- --- --- --- --- --- --- '

begin = System.currentTimeMillis()
def compressedBytesNoDictFirstPage = Zstd.compress(bytesFirstPage, compressLevel)
end = System.currentTimeMillis()
println 'compress first page time no dict: ' + (end - begin) + ' ms'
println 'compressed data length: ' + compressedBytesNoDictFirstPage.length
println 'compress ratio: ' + (pageSize / compressedBytesNoDictFirstPage.length)
println '--- --- --- --- --- --- --- --- --- --- --- --- '

// decompress
begin = System.nanoTime()
def uncompressed = new byte[len]
def d1 = Zstd.decompressUsingDict(uncompressed, 0, compressedBytes, 0, compressedBytes.length, dict.dictBytes)
assert d1 == len
end = System.nanoTime()
println 'decompress time use dict: ' + (end - begin) / 1000 + ' us'
println 'uncompressed data length: ' + uncompressed.length
println 'uncompressed data last 10 bytes: ' + uncompressed[-10..-1]
println '--- --- --- --- --- --- --- --- --- --- --- --- '

begin = System.nanoTime()
def uncompressedFirstPage = new byte[pageSize]
def d2 = Zstd.decompressUsingDict(uncompressedFirstPage, 0, compressedBytesFirstPage, 0, compressedBytesFirstPage.length, dict.dictBytes)
assert d2 == pageSize
end = System.nanoTime()
println 'decompress first page time use dict: ' + (end - begin) / 1000 + ' us'
println 'uncompressed data length: ' + uncompressedFirstPage.length
println 'uncompressed data last 10 bytes: ' + uncompressedFirstPage[-10..-1]
println '--- --- --- --- --- --- --- --- --- --- --- --- '

begin = System.nanoTime()
def uncompressedNoDict = new byte[len]
def d3 = Zstd.decompress(uncompressedNoDict, compressedBytesNoDict)
assert d3 == len
end = System.nanoTime()
println 'decompress time no dict: ' + (end - begin) / 1000 + ' us'
println 'uncompressed data length: ' + uncompressedNoDict.length
println 'uncompressed data last 10 bytes: ' + uncompressedNoDict[-10..-1]
println '--- --- --- --- --- --- --- --- --- --- --- --- '

begin = System.nanoTime()
def uncompressedNoDictFirstPage = new byte[pageSize]
def d4 = Zstd.decompress(uncompressedNoDictFirstPage, compressedBytesNoDictFirstPage)
assert d4 == pageSize
end = System.nanoTime()
println 'decompress first page time no dict: ' + (end - begin) / 1000 + ' us'
println 'uncompressed data length: ' + uncompressedNoDictFirstPage.length
println 'uncompressed data last 10 bytes: ' + uncompressedNoDictFirstPage[-10..-1]
println '--- --- --- --- --- --- --- --- --- --- --- --- '
