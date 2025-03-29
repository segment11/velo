package io.velo.type

import spock.lang.Specification

class RedisBitSetTest extends Specification {
    def 'test get and set'() {
        given:
        def values = new byte[16]
        byte[] nullValues = null

        def bs = new RedisBitSet(values)
        def bs1 = new RedisBitSet(nullValues)

        expect:
        bs.valueBytes == new byte[16]
        bs1.valueBytes == new byte[8]
        !bs.get(0)
        !bs1.get(0)
        !bs.get(200)

        when:
        def result = bs.set(0, true)
        def result1 = bs1.set(0, true)
        println result
        println result1
        then:
        !result.isExpanded()
        result.isChanged()
        !result.oldBit1
        result1.isExpanded()
        result1.isChanged()
        !result1.oldBit1

        when:
        result = bs.set(0, true)
        then:
        !result.isChanged()
        bs.get(0)

        when:
        bs.set(0, false)
        result = bs.set(0, false)
        then:
        !result.isChanged()
        !bs.get(0)

        when:
        // 16 * 8 = 128 < 200
        result = bs.set(200, true)
        then:
        result.isExpanded()
        result.isChanged()
        !result.oldBit1

        when:
        result = bs.set(200, false)
        then:
        result.oldBit1
    }
}
