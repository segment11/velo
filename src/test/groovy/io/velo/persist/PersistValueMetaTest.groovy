package io.velo.persist

import spock.lang.Specification

class PersistValueMetaTest extends Specification {
    def 'test is pvm'() {
        given:
        def bytes = new byte[PersistValueMeta.ENCODED_LENGTH]

        expect:
        PersistValueMeta.isPvm(bytes)
        !PersistValueMeta.isPvm(new byte[10])

        when:
        bytes[0] = -1
        then:
        !PersistValueMeta.isPvm(bytes)
    }

    def 'test encode'() {
        given:
        def one = new PersistValueMeta()
        one.shortType = (byte) 0
        one.subBlockIndex = (byte) 0
        one.segmentIndex = 10
        one.segmentOffset = 10

        println one.shortString()

        when:
        def encoded = one.encode()
        then:
        PersistValueMeta.isPvm(encoded)
        PersistValueMeta.decode(encoded).toString() == one.toString()
        one.isTargetSegment(10, (byte) 0, 10)
        !one.isTargetSegment(11, (byte) 0, 10)
        !one.isTargetSegment(10, (byte) 1, 10)
        !one.isTargetSegment(10, (byte) 0, 11)
    }

    def 'test decode rejects negative subBlockIndex'() {
        given:
        def pvm = new PersistValueMeta()
        pvm.shortType = (byte) 0
        pvm.subBlockIndex = (byte) 0
        pvm.segmentIndex = 1
        pvm.segmentOffset = 100
        def bytes = pvm.encode()

        // corrupt subBlockIndex at offset 3 to -1
        bytes[3] = (byte) -1

        when:
        PersistValueMeta.decode(bytes)
        then:
        def e = thrown(PersistValueMetaCorruptedException)
        e.message.contains('subBlockIndex')
    }

    def 'test decode rejects subBlockIndex >= MAX_BLOCK_NUMBER'() {
        given:
        def pvm = new PersistValueMeta()
        pvm.shortType = (byte) 0
        pvm.subBlockIndex = (byte) 0
        pvm.segmentIndex = 1
        pvm.segmentOffset = 100
        def bytes = pvm.encode()

        // corrupt subBlockIndex at offset 3 to SegmentBatch.MAX_BLOCK_NUMBER
        bytes[3] = (byte) SegmentBatch.MAX_BLOCK_NUMBER

        when:
        PersistValueMeta.decode(bytes)
        then:
        def e = thrown(PersistValueMetaCorruptedException)
        e.message.contains('subBlockIndex')
    }

    def 'test decode rejects negative segmentIndex'() {
        given:
        def pvm = new PersistValueMeta()
        pvm.shortType = (byte) 0
        pvm.subBlockIndex = (byte) 1
        pvm.segmentIndex = 5
        pvm.segmentOffset = 100
        def bytes = pvm.encode()

        // corrupt segmentIndex at offset 4..7 to -1 (0xFFFFFFFF)
        bytes[4] = (byte) 0xFF
        bytes[5] = (byte) 0xFF
        bytes[6] = (byte) 0xFF
        bytes[7] = (byte) 0xFF

        when:
        PersistValueMeta.decode(bytes)
        then:
        def e = thrown(PersistValueMetaCorruptedException)
        e.message.contains('segmentIndex')
    }

    def 'test decode rejects negative segmentOffset'() {
        given:
        def pvm = new PersistValueMeta()
        pvm.shortType = (byte) 0
        pvm.subBlockIndex = (byte) 1
        pvm.segmentIndex = 5
        pvm.segmentOffset = 100
        def bytes = pvm.encode()

        // corrupt segmentOffset at offset 8..11 to -1 (0xFFFFFFFF)
        bytes[8] = (byte) 0xFF
        bytes[9] = (byte) 0xFF
        bytes[10] = (byte) 0xFF
        bytes[11] = (byte) 0xFF

        when:
        PersistValueMeta.decode(bytes)
        then:
        def e = thrown(PersistValueMetaCorruptedException)
        e.message.contains('segmentOffset')
    }

    def 'test decode accepts valid PVM'() {
        given:
        def pvm = new PersistValueMeta()
        pvm.shortType = (byte) 0
        pvm.subBlockIndex = (byte) 3
        pvm.segmentIndex = 100
        pvm.segmentOffset = 500
        def bytes = pvm.encode()

        when:
        def decoded = PersistValueMeta.decode(bytes)
        then:
        decoded.subBlockIndex == 3
        decoded.segmentIndex == 100
        decoded.segmentOffset == 500
    }

    def 'test some branches'() {
        given:
        def one = new PersistValueMeta()
        one.key = 'a'

        when:
        def cellCost = one.cellCostInKeyBucket()
        then:
        cellCost == 1

        when:
        one.extendBytes = new byte[Byte.MAX_VALUE + 1]
        boolean exception = false
        try {
            one.cellCostInKeyBucket()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        one.extendBytes = new byte[Byte.MAX_VALUE]
        exception = false
        try {
            one.cellCostInKeyBucket()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        !exception
    }
}
