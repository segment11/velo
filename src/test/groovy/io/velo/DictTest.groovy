package io.velo


import spock.lang.Specification

import java.nio.ByteBuffer

class DictTest extends Specification {
    def 'test all'() {
        given:
        def dict = new Dict()
        println dict
        println dict.hashCode()

        dict.seq = Dict.SELF_ZSTD_DICT_SEQ
        dict.dictBytes = new byte[10]

        def dict2 = new Dict()
        dict2.seq = 0
        dict2.createdTime = System.currentTimeMillis()
        println dict2.createdTime

        Dict.GLOBAL_ZSTD_DICT.dictBytes = new byte[1]

        expect:
        Dict.SELF_ZSTD_DICT.seq == Dict.SELF_ZSTD_DICT_SEQ
        Dict.GLOBAL_ZSTD_DICT.seq == Dict.GLOBAL_ZSTD_DICT_SEQ
        !Dict.GLOBAL_ZSTD_DICT.hasDictBytes()
        dict.equals(dict)
        dict == Dict.SELF_ZSTD_DICT
        !dict.equals(null)
        dict != new String('xxx')
        dict != dict2

        when:
        HashSet<Integer> seqSet = []
        10000.times {
            seqSet << Dict.generateRandomSeq()
        }
        then:
        // random may conflict
        seqSet.size() == 10000 || seqSet.size() == 9999
    }

    def 'test global dict'() {
        given:
        Dict.GLOBAL_ZSTD_DICT.dictBytes = new byte[1]

        def file = new File('dict-global-test.dat')
        file.bytes = 'test'.bytes

        Dict.resetGlobalDictBytesByFile(file, false)
        file.delete()
        Dict.resetGlobalDictBytesByFile(file, true)

        expect:
        Dict.GLOBAL_ZSTD_DICT.hasDictBytes()
        Dict.GLOBAL_ZSTD_DICT.dictBytes == 'test'.bytes

        when:
        file.bytes = ('test' * 5000).bytes
        boolean exception = false
        try {
            Dict.resetGlobalDictBytesByFile(file, false)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        Dict.resetGlobalDictBytes('test'.bytes, true)
        then:
        Dict.GLOBAL_ZSTD_DICT.dictBytes == 'test'.bytes

        when:
        exception = false
        try {
            Dict.resetGlobalDictBytes(new byte[0], true)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            Dict.resetGlobalDictBytes(('test' * 5000).bytes, true)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        // not change
        Dict.resetGlobalDictBytes('test'.bytes, false)
        then:
        Dict.GLOBAL_ZSTD_DICT.dictBytes == 'test'.bytes

        when:
        exception = false
        try {
            Dict.resetGlobalDictBytes('test2'.bytes, false)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception
        Dict.GLOBAL_ZSTD_DICT.dictBytes != 'test2'.bytes
    }

    def 'test decode'() {
        given:
        def dict = new Dict(new byte[10])
        def keyPrefix = 'test'

        def encoded = dict.encode(keyPrefix)

        def dictWithKeyPrefixOrSuffix = Dict.decode(new DataInputStream(new ByteArrayInputStream(encoded)))
        println dictWithKeyPrefixOrSuffix.toString()
        def dict2 = dictWithKeyPrefixOrSuffix.dict()

        expect:
        dict.encodeLength(keyPrefix) == encoded.length
        dictWithKeyPrefixOrSuffix.keyPrefixOrSuffix() == keyPrefix
        dict == dict2
        dict2.dictBytes == dict.dictBytes

        when:
        def dict3 = Dict.decode(new DataInputStream(new ByteArrayInputStream(new byte[3])))
        then:
        dict3 == null

        when:
        def dict4 = Dict.decode(new DataInputStream(new ByteArrayInputStream(new byte[4])))
        then:
        dict4 == null

        when:
        byte[] testEncodedBytes = new byte[encoded.length]
        def buffer = ByteBuffer.wrap(testEncodedBytes)
        buffer.putInt(0, encoded.length - 4)
        buffer.putShort(4 + 4 + 8, (short) (CompressedValue.KEY_MAX_LENGTH + 1))
        boolean exception = false
        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        buffer.putShort(4 + 4 + 8, (short) 0)
        exception = false
        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        buffer.putInt(0, encoded.length - 4 - 1)
        buffer.putShort(4 + 4 + 8, (short) keyPrefix.length())
        exception = false
        try {
            Dict.decode(new DataInputStream(new ByteArrayInputStream(testEncodedBytes)))
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            new Dict(new byte[Short.MAX_VALUE + 1])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
