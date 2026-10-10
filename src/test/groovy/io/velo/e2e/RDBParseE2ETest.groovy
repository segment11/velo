package io.velo.e2e

import com.moilioncircle.redis.replicator.util.ByteArray
import com.moilioncircle.redis.replicator.util.CRC64
import com.moilioncircle.redis.replicator.util.Lzf
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.velo.command.RDBCallback
import io.velo.command.VeloRDBImporter
import io.velo.rdb.RDBParser
import io.velo.test.tools.RedisServer
import io.velo.type.RedisHH
import io.velo.type.RedisHashKeys
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import redis.clients.jedis.Jedis
import redis.clients.jedis.StreamEntryID
import spock.lang.Shared
import spock.lang.Specification

import java.nio.file.Files

/**
 * End to end test for {@link RDBParser}: a real redis-server writes a dump.rdb,
 * then the raw rdb bytes are parsed by {@code RDBParser}, to cover as many rdb
 * types and listpack entry encodings as redis is able to produce.
 *
 * <p>Two entry points are exercised with the very same data set:
 * <ol>
 *     <li>the whole {@code dump.rdb} file, opcode by opcode, plus its crc64</li>
 *     <li>the production path, {@code jedis.dump(key)} fed to {@link VeloRDBImporter}</li>
 * </ol>
 *
 * <p>Encodings a modern redis-server never emits any more (ziplist based types,
 * the legacy {@code RDB_TYPE_ZSET} string score format) are covered by payloads
 * crafted byte by byte in {@code test parse legacy rdb type encodings}.
 */
class RDBParseE2ETest extends Specification {
    // listpack integer entry encodings, grouped by the encoding redis uses for them
    private static final List<String> SevenBitUint = ['0', '127']
    private static final List<String> ThirteenBitInt = ['-1', '-4096', '128', '4095']
    private static final List<String> SixteenBitInt = ['-4097', '-32768', '4096', '32767']
    private static final List<String> TwentyFourBitInt = ['-32769', '-8388608', '32768', '8388607']
    private static final List<String> ThirtyTwoBitInt = ['-8388609', '-2147483648', '8388608', '2147483647']
    private static final List<String> SixtyFourBitInt = ['4294967295', '-5000000000']
    private static final List<String> AllInts = SevenBitUint + ThirteenBitInt + SixteenBitInt + TwentyFourBitInt + ThirtyTwoBitInt + SixtyFourBitInt

    // listpack string entry encodings: 6bit str (<=63), 12bit str (64..4095), 32bit str (>4095)
    private static final String SixBitStr = 'a' * 63
    private static final String TwelveBitStr = 'b' * 100
    private static final String ThirtyTwoBitStr = 'c' * 5000
    // entry size = value len + 5 for a 32bit str, so the backlen width can be covered
    private static final String BackLen2Str = 'd' * 123 // 128 -> 2 bytes backlen
    private static final String BackLen3Str = 'e' * 16381 // 16386 -> 3 bytes backlen
    private static final String BackLen4Str = 'f' * 2097146 // 2097151 -> 4 bytes backlen

    @Shared
    RedisServer redisServer
    @Shared
    Jedis jedis
    @Shared
    File dir
    @Shared
    boolean skipped = false

    /** key to expected value, strings are byte[], the other types are groovy collections */
    @Shared
    Map<String, Object> expected = [:]
    /** key to the approximate expire at millis written by redis into the rdb */
    @Shared
    Map<String, Long> expectedExpireAt = [:]
    @Shared
    List<String> expectedList
    @Shared
    Set<String> expectedSet
    @Shared
    Set<String> expectedSetInt16
    @Shared
    Set<String> expectedSetInt32
    @Shared
    Set<String> expectedSetInt64
    @Shared
    Map<String, String> expectedHash
    @Shared
    Map<String, Double> expectedZSet

    def setupSpec() {
        if (!RedisServer.isBinExists()) {
            skipped = true
            println 'redis-server bin not exists, skip RDB parse e2e test'
            return
        }

        dir = Files.createTempDirectory('velo-rdb-parse-').toFile()
        redisServer = new RedisServer('rdb-parse').dir(dir.absolutePath).noSave().randomPort()
        Thread.start { redisServer.run() }
        jedis = redisServer.jedis()
        jedis.flushAll()

        // keep listpack encodings for the values used by this test
        jedis.configSet('set-max-listpack-entries', '10000')
        jedis.configSet('zset-max-listpack-entries', '10000')
        jedis.configSet('hash-max-listpack-entries', '10000')
        jedis.configSet('hash-max-listpack-value', '4096')

        // string values: rdb 6bit/14bit/32bit length, lzf compressed, integer encoded
        jedis.set('str:hello', 'hello')
        jedis.set('str:len63', 'x' * 63)
        jedis.set('str:len64', 'x' * 64)
        jedis.set('str:len300', 'y' * 300)
        jedis.set('str:len20000'.bytes, randomBytes(20000))
        jedis.set('str:int', '1234')
        expectString('str:hello', 'hello')
        expectString('str:len63', 'x' * 63)
        expectString('str:len64', 'x' * 64)
        expectString('str:len300', 'y' * 300)
        expectString('str:len20000', randomBytes(20000))
        expectString('str:int', '1234')

        // strings lzf compressed by redis, redis only compresses values longer than 20 bytes
        jedis.set('str:lzf', 'l' * 20000)
        jedis.set('str:lzf:key' + ('k' * 200), 'k' * 20000)
        expectString('str:lzf', 'l' * 20000)
        expectString('str:lzf:key' + ('k' * 200), 'k' * 20000)

        // strings redis stores with the 8 bit, 16 bit and 32 bit integer encoding
        ['str:int8': '-128', 'str:int8:max': '127',
         'str:int16': '-40000', 'str:int16:max': '32767',
         'str:int32': '-2000000000', 'str:int32:max': '2147483647'].each { String key, String value ->
            jedis.set(key, value)
            expectString(key, value)
        }

        // a key longer than 63 bytes and a binary key
        def longKey = 'key:' + 'q' * 200 + ':end'
        jedis.set(longKey, 'v')
        expectString(longKey, 'v')
        def binaryKey = ('key:bin:' + new String(randomBytes(40), 'ISO-8859-1'))
        jedis.set(binaryKey.bytes, randomBytes(20))
        expectString(new String(binaryKey.bytes), randomBytes(20))

        // expire opcodes, redis writes a 4 bytes one for a short ttl and a 8 bytes one for a long ttl
        ['str:ttl:short': 100_000L, 'str:ttl:long': 40L * 365 * 24 * 3600 * 1000].each { String key, long ttlMs ->
            jedis.psetex(key, ttlMs, 'v')
            expected.put(key, 'v'.bytes)
            expectedExpireAt.put(key, System.currentTimeMillis() + ttlMs)
        }

        // list, quicklist2 with listpack nodes and one plain node
        expectedList = AllInts + [SixBitStr, TwelveBitStr, ThirtyTwoBitStr, 'w' * 20000]
        jedis.rpush('list:mixed', expectedList as String[])
        expected.put('list:mixed', expectedList)

        // a list with more entries than list-max-listpack-size, so several listpack nodes
        def listNodes = (1..300).collect { 'node:' + it }
        jedis.rpush('list:nodes', listNodes as String[])
        expected.put('list:nodes', listNodes)

        // set with mixed members, listpack encoded
        expectedSet = new LinkedHashSet<String>(AllInts)
        expectedSet.addAll([SixBitStr, TwelveBitStr, ThirtyTwoBitStr,
                            BackLen2Str, BackLen3Str, BackLen4Str, 'not-a-number'])
        jedis.sadd('set:mixed', expectedSet as String[])
        expected.put('set:mixed', expectedSet)

        // all-integer sets are intset encoded, with 2 bytes, 4 bytes and 8 bytes members
        expectedSetInt16 = (1..100).collect { String.valueOf(it) } as Set<String>
        expectedSetInt16.addAll(['-1', '-32768']) // negative members, still 2 bytes in an intset
        jedis.sadd('set:int16', expectedSetInt16 as String[])
        expected.put('set:int16', expectedSetInt16)
        expectedSetInt32 = new LinkedHashSet<String>(expectedSetInt16)
        expectedSetInt32.addAll(['32768', '2147483647', '-2147483648'])
        jedis.sadd('set:int32', expectedSetInt32 as String[])
        expected.put('set:int32', expectedSetInt32)
        expectedSetInt64 = ['4294967296', '9223372036854775807', '-9223372036854775808', '-1'] as Set<String>
        jedis.sadd('set:int64', expectedSetInt64 as String[])
        expected.put('set:int64', expectedSetInt64)

        // hash, listpack encoded
        expectedHash = [
                'f:short'  : 'v',
                'f:str6b'  : SixBitStr,
                'f:str12b' : TwelveBitStr,
        ]
        addToMap(expectedHash, 'f:int7b', SevenBitUint)
        addToMap(expectedHash, 'f:int13', ThirteenBitInt)
        addToMap(expectedHash, 'f:int16', SixteenBitInt)
        addToMap(expectedHash, 'f:int24', TwentyFourBitInt)
        addToMap(expectedHash, 'f:int32', ThirtyTwoBitInt)
        addToMap(expectedHash, 'f:int64', SixtyFourBitInt)
        jedis.hset('hash:mixed', expectedHash)
        expected.put('hash:mixed', expectedHash)

        // zset, listpack encoded, scores and members of every encoding
        expectedZSet = [
                'm:1'        : 1.0d,
                'm:1.5'      : 1.5d,
                'm:-1'       : -1.0d,
                'm:inf'      : Double.POSITIVE_INFINITY,
                'm:-inf'     : Double.NEGATIVE_INFINITY,
                SixBitStr    : 1.0d,
                TwelveBitStr : 2.0d,
                ThirtyTwoBitStr: 3.0d,
        ]
        AllInts.eachWithIndex { String v, int i -> expectedZSet.put(v, i + 0.25d) }
        expectedZSet.each { String member, Double score -> jedis.zadd('zset:mixed', score, member) }
        expected.put('zset:mixed', expectedZSet)

        // a zset with a big and a very precise score
        def bigScoreZSet = ['b:1': 1.7976931348623157e+307d, 'b:2': 3.141592653589793d, 'b:3': 1.0e-300d]
        bigScoreZSet.each { String member, Double score -> jedis.zadd('zset:score', score, member) }
        expected.put('zset:score', bigScoreZSet)

        // back to the default thresholds, so the big values below get hashtable / skiplist / plain
        // quicklist encodings instead of the listpack ones covered above
        jedis.configSet('set-max-listpack-entries', '128')
        jedis.configSet('zset-max-listpack-entries', '128')
        jedis.configSet('hash-max-listpack-entries', '128')

        def bigSet = (1..200).collect { 's:' + it } as Set<String>
        jedis.sadd('set:big', bigSet as String[])
        expected.put('set:big', bigSet)

        // more than 2^14 members, so the member count uses the 32 bit rdb length encoding
        def hugeSet = (1..16384).collect { 'h:' + it } as Set<String>
        jedis.sadd('set:huge', hugeSet as String[])
        expected.put('set:huge', hugeSet)

        def bigHash = (1..200).collectEntries { ['h:' + it, 'v:' + it] } as Map<String, String>
        jedis.hset('hash:big', bigHash)
        expected.put('hash:big', bigHash)

        def bigZSet = (1..200).collectEntries { ['z:' + it, it + 0.5d] } as Map<String, Double>
        bigZSet.each { String member, Double score -> jedis.zadd('zset:big', score, member) }
        expected.put('zset:big', bigZSet)

        // list-compress-depth 0 makes every quicklist node a plain node
        jedis.configSet('list-compress-depth', '0')
        def plainList = (1..300).collect { 'plain:' + it }
        jedis.rpush('list:plain', plainList as String[])
        expected.put('list:plain', plainList)
        jedis.configSet('list-compress-depth', '1')

        jedis.save()
    }

    def cleanupSpec() {
        try {
            jedis?.close()
        } catch (Exception ignored) {
        }
        try {
            redisServer?.stop()
        } catch (Exception ignored) {
        }
        try {
            dir?.deleteDir()
        } catch (Exception ignored) {
        }
    }

    private void expectString(String key, String value) {
        expected.put(key, value.bytes)
    }

    private void expectString(String key, byte[] value) {
        expected.put(key, value)
    }

    def 'test parse rdb file generated by redis-server'() {
        given:
        if (skipped) {
            println 'skip test'
            return
        }

        when: 'parse the whole rdb file'
        def parseResult = parseRdbFile(rdbFile('dump.rdb'))
        def parsed = parseResult.result

        then: 'every key is parsed, with the value redis stored'
        parsed.keySet() == expected.keySet()
        expected.every { String key, Object value ->
            assert parsed[key] == value: "value of $key is ${parsed[key]}, expected $value"
            true
        }

        and: 'the expire times written by redis are read back'
        parseResult.expireAt.keySet() == expectedExpireAt.keySet()
        expectedExpireAt.every { String key, Long expireAt ->
            assert Math.abs(parseResult.expireAt[key] - expireAt) < 3000: "expire of $key is ${parseResult.expireAt[key]}, expected $expireAt"
            true
        }
    }

    def 'test restore rdb payload dumped by jedis'() {
        given: 'the payload of every key, as sent by a RESTORE command'
        if (skipped) {
            println 'skip test'
            return
        }
        def importer = new VeloRDBImporter()

        when: 'restore every dumped payload'
        def restored = [:]
        expected.keySet().each { String key ->
            def payload = jedis.dump(key.bytes)
            assert payload != null: "no dump payload for $key"
            restored.put(key, restoreOne(importer, payload))
        }

        then: 'every value is restored exactly'
        restored.keySet() == expected.keySet()
        expected.every { String key, Object value ->
            assert restored[key] == value: "restored value of $key is ${restored[key]}, expected $value"
            true
        }
    }

    def 'test restore payload with a corrupted crc64'() {
        given:
        if (skipped) {
            println 'skip test'
            return
        }
        def payload = jedis.dump('str:hello'.bytes)

        when:
        def broken = payload.clone()
        broken[broken.length - 1] = (broken[broken.length - 1] ^ 0x01) as byte
        new VeloRDBImporter().restore(Unpooled.wrappedBuffer(broken), new CollectCallback())

        then:
        def e = thrown(IllegalArgumentException)
        e.message.startsWith('CRC64 mismatch')
    }

    def 'test parse rdb file with stream type'() {
        given: 'a dedicated server, dbfilename is a protected config'
        if (skipped) {
            println 'skip test'
            return
        }
        def streamDir = Files.createTempDirectory('velo-rdb-stream-').toFile()
        def streamServer = new RedisServer('rdb-stream').dir(streamDir.absolutePath).noSave().randomPort()
        Thread.start { streamServer.run() }
        def streamJedis = streamServer.jedis()
        def streamRdb = new File(streamDir, 'dump.rdb')

        when:
        streamJedis.xadd('stream:1', StreamEntryID.NEW_ENTRY, ['f1': 'v1', 'f2': 'v2'] as Map<String, String>)
        streamJedis.save()

        and:
        parseRdbFile(streamRdb)

        then: 'streams are not supported yet'
        def e = thrown(IllegalArgumentException)
        e.message.startsWith('Unsupported type stream yet')

        cleanup:
        streamJedis?.close()
        streamServer?.stop()
        streamDir?.deleteDir()
    }

    /**
     * RDB types redis 7 never writes any more, but old clients still dump them, and a
     * RESTORE of such a payload goes through {@link RDBParser}.
     */
    def 'test parse legacy rdb type encodings'() {
        given:
        if (skipped) {
            println 'skip test'
            return
        }
        def importer = new VeloRDBImporter()

        when: 'the legacy zset type, scores are stored as a length prefixed ascii string'
        def legacyZSet = new ByteArrayOutputStream()
        writeLength(legacyZSet, 3)
        writeRdbStringBytes(legacyZSet, 'a')
        writeDouble(legacyZSet, 1.5d)
        writeRdbStringBytes(legacyZSet, 'b')
        writeDouble(legacyZSet, -1.5d)
        writeRdbStringBytes(legacyZSet, 'inf-member')
        writeDouble(legacyZSet, Double.POSITIVE_INFINITY)

        then:
        restoreOne(importer, rdbPayload(3, legacyZSet.toByteArray())) ==
                ['a': 1.5d, 'b': -1.5d, 'inf-member': Double.POSITIVE_INFINITY]

        when: 'a ziplist encoded list'
        def listElements = [['a', 'b'], ['127', '-1'], ['c' * 100]]
        def ziplist = buildZiplist(listElements.flatten() as List<String>)
        def outList = new ByteArrayOutputStream()
        writeRdbStringBytes(outList, ziplist)

        then:
        restoreOne(importer, rdbPayload(10, outList.toByteArray())) == listElements.flatten()

        when: 'a ziplist encoded hash'
        def hashPairs = ['f1': 'v1', 'f2': 'x' * 300, '127': '-1']
        def flatPairs = hashPairs.collectMany { String k, String v -> [k, v] }
        def outHash = new ByteArrayOutputStream()
        writeRdbStringBytes(outHash, buildZiplist(flatPairs))

        then:
        restoreOne(importer, rdbPayload(13, outHash.toByteArray())) == hashPairs

        when: 'a ziplist encoded zset, member and score are stored as an adjacent pair'
        def zsetOut = new ByteArrayOutputStream()
        writeRdbStringBytes(zsetOut, buildZiplist(['a', '1.5', 'b', '-2.5', 'c', 'inf', 'd', '-inf']))
        def zsetParsed = restoreOne(importer, rdbPayload(12, zsetOut.toByteArray())) as Map<String, Double>

        then:
        zsetParsed == ['a': 1.5d, 'b': -2.5d, 'c': Double.POSITIVE_INFINITY, 'd': Double.NEGATIVE_INFINITY]

        when: 'a quicklist1, every node is a ziplist without a container byte'
        def quickListNodes = new ByteArrayOutputStream()
        writeLength(quickListNodes, 2)
        writeRdbStringBytes(quickListNodes, buildZiplist(['a', 'b']))
        writeRdbStringBytes(quickListNodes, buildZiplist(['c' * 300, 'd']))

        then:
        restoreOne(importer, rdbPayload(14, quickListNodes.toByteArray())) == ['a', 'b', 'c' * 300, 'd']
    }

    /**
     * Restore a single RESTORE style payload and normalize the value the same way
     * {@link CollectCallback} does, so both entry points can be compared.
     */
    private static Object restoreOne(VeloRDBImporter importer, byte[] payload) {
        def callback = new CollectCallback()
        importer.restore(Unpooled.wrappedBuffer(payload), callback)
        callback.lastValue
    }

    private static void addToMap(Map<String, String> map, String prefix, List<String> values) {
        values.eachWithIndex { String v, int i -> map.put("$prefix:$i".toString(), v) }
    }

    private File rdbFile(String name) {
        def f = new File(dir, name)
        assert f.exists()
        assert f.length() > 0
        f
    }

    private static byte[] randomBytes(int len) {
        def random = new Random(42L)
        def bytes = new byte[len]
        random.nextBytes(bytes)
        bytes
    }

    private static long readLength(ByteBuf buf) {
        int first = buf.readUnsignedByte()
        int type = (first & 0xC0) >> 6
        switch (type) {
            case 0:
                return first & 0x3F
            case 1:
                return ((first & 0x3F) << 8) | buf.readUnsignedByte()
            case 2:
                return buf.readInt() & 0xFFFFFFFFL
            default:
                int enc = first & 0x3F
                assert enc >= 0 && enc <= 3: "unknown special encoding $enc"
                return enc
        }
    }

    private static byte[] readRdbString(ByteBuf buf) {
        int first = buf.getUnsignedByte(buf.readerIndex())
        boolean isSpecial = (first & 0xC0) == 0xC0
        long len = readLength(buf)
        if (isSpecial) {
            switch ((int) len) {
                case 0:
                    return Integer.toString(buf.readByte()).bytes
                case 1:
                    return Integer.toString(buf.readShortLE()).bytes
                case 2:
                    return Integer.toString(buf.readIntLE()).bytes
                case 3:
                    int compressedLen = (int) readLength(buf)
                    int uncompressedLen = (int) readLength(buf)
                    def compressed = new byte[compressedLen]
                    buf.readBytes(compressed)
                    return concat(Lzf.decode(new ByteArray(compressed), uncompressedLen).iterator(), uncompressedLen)
                default:
                    throw new IllegalArgumentException("Unknown integer encoding: $len")
            }
        }
        def out = new byte[(int) len]
        buf.readBytes(out)
        out
    }

    private static byte[] concat(Iterator<byte[]> it, int totalLen) {
        def out = new byte[totalLen]
        int pos = 0
        while (it.hasNext()) {
            def bytes = it.next()
            System.arraycopy(bytes, 0, out, pos, bytes.length)
            pos += bytes.length
        }
        assert pos == totalLen
        out
    }

    /**
     * Parse a whole rdb file opcode by opcode, delegating every key value pair to {@link RDBParser}.
     *
     * @param file the rdb file
     * @return the parsed key values and the expire at millis of the keys having one
     */
    private static ParseResult parseRdbFile(File file) {
        def allBytes = Files.readAllBytes(file.toPath())
        def header = new byte[9]
        def crc = CRC64.crc64(allBytes, 0, allBytes.length - 8)
        def expectedCrc = readLongLE(allBytes, allBytes.length - 8)

        def buf = Unpooled.wrappedBuffer(allBytes)

        assert buf.readableBytes() > 9
        buf.readBytes(header)
        def version = (header[5] & 0xFF) * 100 + (header[6] & 0xFF) * 10 + (header[7] & 0xFF)
        assert new String(header, 0, 5) == 'REDIS'
        assert version >= RDBParser.RDB_MIN_VERSION
        assert crc == expectedCrc: "crc64 of $file is $crc, expected $expectedCrc"

        def parser = new RDBParser()
        def callback = new CollectCallback()
        def parseResult = new ParseResult()
        parseResult.result = callback.result // same map, the callback fills it
        Long expireAt = null
        while (buf.isReadable()) {
            int type = buf.readUnsignedByte()
            if (type == 0xFF) { // eof
                break
            } else if (type == 0xFE) { // select db
                readLength(buf)
            } else if (type == 0xFB) { // resize db hints
                readLength(buf)
                readLength(buf)
            } else if (type == 0xFC) { // expire time in ms
                expireAt = buf.readLongLE()
            } else if (type == 0xFD) { // expire time in s
                expireAt = readLength(buf) * 1000
            } else if (type == 0xFA) { // aux field
                readRdbString(buf)
                readRdbString(buf)
            } else if (type == 0xF9) { // lfu freq
                buf.skipBytes(1)
            } else if (type == 0xF8) { // lru idle time
                buf.skipBytes(1)
            } else if (type == 0xF7) { // module aux
                readRdbString(buf)
            } else if (type == 0xF5) { // function
                readRdbString(buf)
            } else if (type == 0xF4) { // slot info
                readLength(buf)
                readLength(buf)
            } else {
                // not an opcode, so this byte is the type of a `<type><key><value>` pair
                buf.readerIndex(buf.readerIndex() - 1)
                int typeByte = buf.readUnsignedByte()
                callback.key = new String(readRdbString(buf))
                // RDBParser.readEntry reads `<type><value>`, so replay the type byte
                // in front of the value bytes without copying the rest of the file
                int entryStart = buf.readerIndex()
                def valueBuf = buf.slice(buf.readerIndex(), buf.readableBytes())
                def entryBuf = Unpooled.wrappedUnmodifiableBuffer(
                        Unpooled.wrappedBuffer([(byte) typeByte] as byte[]), valueBuf)
                parser.readEntry(entryBuf, callback)
                buf.readerIndex(entryStart + entryBuf.readerIndex() - 1) // -1, the type byte is only replayed
                if (expireAt != null) {
                    parseResult.expireAt.put(callback.key, expireAt)
                    expireAt = null
                }
            }
        }

        // only the crc64 checksum should be left
        assert buf.readableBytes() == 8

        parseResult
    }

    static class ParseResult {
        Map<String, Object> result = [:]
        Map<String, Long> expireAt = [:]
    }

    private static long readLongLE(byte[] bytes, int offset) {
        long value = 0
        for (int i = 7; i >= 0; i--) {
            value = (value << 8) | (bytes[offset + i] & 0xFFL)
        }
        value
    }

    /**
     * Append a plain rdb string, the smallest of the 6bit / 14bit / 32bit length encodings.
     */
    private static void writeRdbStringBytes(ByteArrayOutputStream out, String value) {
        writeRdbStringBytes(out, value.bytes)
    }

    /**
     * Append a plain rdb string, the smallest of the 6bit / 14bit / 32bit length encodings.
     */
    private static void writeRdbStringBytes(ByteArrayOutputStream out, byte[] bytes) {
        if (bytes.length < 1 << 6) {
            out.write(bytes.length)
        } else if (bytes.length < 1 << 14) {
            out.write(((bytes.length >> 8) & 0x3F) | 0x40)
            out.write(bytes.length & 0xFF)
        } else {
            out.write(0x80)
            writeIntLE(out, bytes.length)
        }
        out.write(bytes)
    }

    private static void writeLength(ByteArrayOutputStream out, int length) {
        if (length < 1 << 6) {
            out.write(length)
        } else if (length < 1 << 14) {
            out.write(((length >> 8) & 0x3F) | 0x40)
            out.write(length & 0xFF)
        } else {
            out.write(0x80)
            writeIntLE(out, length)
        }
    }

    /**
     * A score of the legacy RDB_TYPE_ZSET format, a 1 byte length then the ascii
     * representation, 253 / 254 / 255 are the nan / +inf / -inf specials.
     */
    private static void writeDouble(ByteArrayOutputStream out, double score) {
        if (Double.isNaN(score)) {
            out.write(253)
        } else if (score == Double.POSITIVE_INFINITY) {
            out.write(254)
        } else if (score == Double.NEGATIVE_INFINITY) {
            out.write(255)
        } else {
            def bytes = score.toString().bytes
            out.write(bytes.length)
            out.write(bytes)
        }
    }

    private static void writeIntLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF)
        out.write((value >> 8) & 0xFF)
        out.write((value >> 16) & 0xFF)
        out.write((value >> 24) & 0xFF)
    }

    private static void writeShortLE(ByteArrayOutputStream out, int value) {
        out.write(value & 0xFF)
        out.write((value >> 8) & 0xFF)
    }

    /**
     * Build a ziplist, the 10 bytes header plus one entry per value.
     * Every entry is a plain string, the integer encodings are covered by the
     * listpack encodings a real redis-server produces.
     *
     * @param values the strings to encode, in order
     * @return the ziplist bytes
     */
    private static byte[] buildZiplist(List<String> values) {
        def body = new ByteArrayOutputStream()
        int prevEntryLen = 0
        values.each { String value ->
            def bytes = value.bytes
            if (prevEntryLen < 254) {
                body.write(prevEntryLen)
            } else {
                body.write(254)
                writeIntLE(body, prevEntryLen)
            }
            int entryStart = body.size()
            if (bytes.length < 1 << 6) {
                body.write(bytes.length)
            } else if (bytes.length < 1 << 14) {
                body.write(((bytes.length >> 8) & 0x3F) | 0x40)
                body.write(bytes.length & 0xFF)
            } else {
                body.write(0x80)
                writeIntLE(body, bytes.length)
            }
            body.write(bytes)
            prevEntryLen = body.size() - entryStart
        }

        def out = new ByteArrayOutputStream()
        writeIntLE(out, 10 + body.size()) // zlbytes
        writeIntLE(out, body.size()) // zltail
        writeShortLE(out, values.size()) // zllen
        out.write(body.toByteArray())
        out.toByteArray()
    }

    /**
     * Build a RESTORE style payload: {@code <type><value>}, rdb version and the crc64 footer.
     *
     * @param type       the rdb type byte
     * @param valueBytes the type specific value bytes
     * @return the payload bytes
     */
    private static byte[] rdbPayload(int type, byte[] valueBytes) {
        def out = new ByteArrayOutputStream()
        out.write(type)
        out.write(valueBytes)
        writeShortLE(out, RDBParser.RDB_VERSION)
        def bytes = out.toByteArray()
        def crc = CRC64.crc64(bytes, 0, bytes.length)
        def withCrc = new ByteArrayOutputStream()
        withCrc.write(bytes)
        for (int i = 0; i < 8; i++) {
            withCrc.write(((crc >> (8 * i)) & 0xFFL) as int)
        }
        withCrc.toByteArray()
    }

    static class CollectCallback implements RDBCallback {
        String key
        Map<String, Object> result = [:]
        Object lastValue

        @Override
        void onString(byte[] valueBytes) {
            println "String: $key -> len=${valueBytes.length}"
            lastValue = valueBytes
            result.put(key, valueBytes)
        }

        @Override
        void onList(RedisList rl) {
            println "List: $key -> size=${rl.size()}"
            lastValue = rl.list.collect { new String(it) }
            result.put(key, lastValue)
        }

        @Override
        void onSet(RedisHashKeys rhk) {
            println "Set: $key -> size=${rhk.size()}"
            lastValue = new HashSet<String>(rhk.set)
            result.put(key, lastValue)
        }

        @Override
        void onZSet(RedisZSet rz) {
            println "ZSet: $key -> size=${rz.size()}"
            lastValue = rz.set.collectEntries { [(it.member()): it.score()] } as Map<String, Double>
            result.put(key, lastValue)
        }

        @Override
        void onHash(RedisHH rhh) {
            println "Hash: $key -> size=${rhh.size()}"
            lastValue = rhh.map.collectEntries { k, v -> [(k): new String(v)] } as Map<String, String>
            result.put(key, lastValue)
        }
    }
}