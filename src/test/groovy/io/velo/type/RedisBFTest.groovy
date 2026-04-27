package io.velo.type

import spock.lang.Specification

import java.nio.ByteBuffer

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

    def 'test isFull handles zero capacity'() {
        given:
        def redisBF = new RedisBF(true)
        redisBF.put('item1')

        when:
        def encoded = redisBF.encode()
        def decodedBF = RedisBF.decode(encoded)

        then:
        decodedBF.listSize() == 1

        when:
        def field = RedisBF.getDeclaredField('list')
        field.setAccessible(true)
        def list = field.get(decodedBF)
        def oneClass = Class.forName('io.velo.type.RedisBF$One')
        def capacityField = oneClass.getDeclaredField('capacity')
        capacityField.setAccessible(true)
        def itemInsertedField = oneClass.getDeclaredField('itemInserted')
        itemInsertedField.setAccessible(true)
        def one = list[0]
        capacityField.set(one, 0)
        itemInsertedField.set(one, 0)

        then:
        noExceptionThrown()
        one.isFull() == true
    }

    def 'test capacity expansion overflow detection'() {
        given:
        def redisBF = new RedisBF(true)

        when:
        def encoded = redisBF.encode()
        def decodedBF = RedisBF.decode(encoded)

        then:
        decodedBF.listSize() == 1

        when:
        def field = RedisBF.getDeclaredField('list')
        field.setAccessible(true)
        def list = field.get(decodedBF)
        def oneClass = Class.forName('io.velo.type.RedisBF$One')
        def capacityField = oneClass.getDeclaredField('capacity')
        capacityField.setAccessible(true)
        def itemInsertedField = oneClass.getDeclaredField('itemInserted')
        itemInsertedField.setAccessible(true)
        def one = list[0]
        def largeCapacity = (int) (Integer.MAX_VALUE / 2.0) + 1
        capacityField.set(one, largeCapacity)
        itemInsertedField.set(one, (int) (largeCapacity * 0.91))

        then:
        one.isFull()

        when:
        decodedBF.put('trigger_expansion_item')

        then:
        def e = thrown(RuntimeException)
        e.message.contains('overflow')
    }

    def 'test constructor rejects non-positive expansion'() {
        when:
        new RedisBF(1000, 0.01, (byte) 0, false)

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('expansion must be positive')

        when:
        new RedisBF(1000, 0.01, (byte) -1, false)

        then:
        def e2 = thrown(IllegalArgumentException)
        e2.message.contains('expansion must be positive')
    }

    def 'test decode rejects non-positive expansion'() {
        given:
        def bf = new RedisBF(1000, 0.01, (byte) 2, false)
        bf.put('a')
        def encoded = bf.encode()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.put(0, (byte) 0)

        when:
        RedisBF.decode(encoded)

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('expansion must be positive')
    }

    def 'test decode rejects empty sub-filter list'() {
        given:
        def buf = ByteBuffer.allocate(1 + 1 + 8 + 4)
        buf.put((byte) 2)
        buf.put((byte) 0)
        buf.putDouble(0.01)
        buf.putInt(0)

        when:
        RedisBF.decode(buf.array())

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('list size')
    }

    def 'test decode rejects negative sub-filter list size'() {
        given:
        def buf = ByteBuffer.allocate(1 + 1 + 8 + 4)
        buf.put((byte) 2)
        buf.put((byte) 0)
        buf.putDouble(0.01)
        buf.putInt(-1)

        when:
        RedisBF.decode(buf.array())

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('list size')
    }

    def 'test decode rejects sub-filter list exceeding max size'() {
        given:
        def buf = ByteBuffer.allocate(1 + 1 + 8 + 4)
        buf.put((byte) 2)
        buf.put((byte) 0)
        buf.putDouble(0.01)
        buf.putInt(100)

        when:
        RedisBF.decode(buf.array())

        then:
        def e = thrown(IllegalStateException)
        e.message.contains('list size')
    }
}
