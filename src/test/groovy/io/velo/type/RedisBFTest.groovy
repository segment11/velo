package io.velo.type

import spock.lang.Specification

class RedisBFTest extends Specification {
    def items = (0..<100).collect { 'item:' + it }
    def items200 = (0..<200).collect { 'item:' + it }

    def 'test base'() {
        given:
        def redisBF = new RedisBF(true)

        expect:
        redisBF.itemInserted() == 0
        redisBF.capacity() == 100
        redisBF.listSize() == 1

        when:
        items[0..<10].each {
            redisBF.put(it)
        }
        then:
        redisBF.itemInserted() == 10
        items[0..<10].every {
            redisBF.mightContain(it)
        }
        !redisBF.put(items[0])

        when:
        items[10..<100].each {
            redisBF.put(it)
        }
        then:
        // expand
        redisBF.listSize() == 2

        when:
        def redisBF2 = new RedisBF(100, 0.01, (byte) 2, true)
        boolean exception = false
        try {
            items.each {
                redisBF2.put(it)
            }
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def redisBF3 = new RedisBF(10, 0.1, (byte) 2, false)
        exception = false
        try {
            items200.each {
                redisBF3.put(it)
            }
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test encode'() {
        given:
        def redisBF = new RedisBF(true)
        println redisBF.expansion
        println redisBF.nonScaling
        println redisBF.memoryAllocatedEstimate()

        when:
        items200.each {
            redisBF.put(it)
        }
        def encoded = redisBF.encode()
        def redisBF2 = RedisBF.decode(encoded)
        then:
        redisBF2.itemInserted() <= 200
        redisBF2.capacity() == 100 + 200
        redisBF2.listSize() == 2
    }
}
