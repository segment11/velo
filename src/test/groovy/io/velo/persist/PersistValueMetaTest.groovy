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
        println one.positionUuid()

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

    def 'test some branches'() {
        given:
        def one = new PersistValueMeta()
        one.keyBytes = 'a'.bytes

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
