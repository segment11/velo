package io.velo.rdb

import spock.lang.Specification

import java.nio.ByteBuffer

class RedisLzfTest extends Specification {
    def 'test all'() {
        final String libFileName = 'libredis_lzf.so'
        def f = new File('./' + libFileName)
        println f.absolutePath
        if (!f.exists()) {
            println 'lib not found'
            return
        }

        given:
        def redisLzf = RedisLzf.instance

        when:
        def inputBuffer = ByteBuffer.allocateDirect(1024)
        def decompressedBuffer = ByteBuffer.allocateDirect(1024)
        def outputBuffer = ByteBuffer.allocateDirect(1024)
        def input = 'xxxxxyyyyyzzzzz' * 5
        println 'input length: ' + input.length()
        println 'input bytes: ' + input.bytes
        inputBuffer.put(input.bytes)
        inputBuffer.position(0)
        def compressedLength = redisLzf.lzf_compress(inputBuffer, input.length(), outputBuffer, input.length() * 2)
        println 'compressed length: ' + compressedLength
        then:
        compressedLength > 0

        when:
        def compressedBytes = new byte[compressedLength]
        outputBuffer.position(0).get(compressedBytes)
        println 'compressed bytes: ' + compressedBytes
        outputBuffer.position(0)
        def decompressedLength = redisLzf.lzf_decompress(outputBuffer, compressedLength, decompressedBuffer, input.length())
        println 'decompressed length: ' + decompressedLength
        def decompressedBytes = new byte[decompressedLength]
        decompressedBuffer.position(0).get(decompressedBytes)
        println 'decompressed bytes: ' + decompressedBytes
        then:
        decompressedLength == input.length()
        decompressedBuffer.slice(0, decompressedLength) == inputBuffer.slice(0, decompressedLength)
    }
}
