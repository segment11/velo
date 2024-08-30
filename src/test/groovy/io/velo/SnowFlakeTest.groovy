package io.velo


import spock.lang.Specification

class SnowFlakeTest extends Specification {
    def 'test all'() {
        given:
        def snowFlake = new SnowFlake(1, 1)
        List<Long> idSet = []
        def times = 1 << (10 + 10)

        when:
        def beginT = System.nanoTime()
        for (int i = 0; i < times; i++) {
            def id = snowFlake.nextId()
            if (i == 0 || i == times - 1) {
                idSet << id
            }
        }
        def endT = System.nanoTime()
        println 'Times: ' + times + ', Cost time: ' + (endT - beginT) / 1000 + 'us'
        println 'Last next id: ' + snowFlake.lastNextId
        then:
        idSet.size() == 2
        idSet.last() > idSet.first()
    }

    def 'test exception'() {
        when:
        boolean exception = false
        try {
            new SnowFlake(-1, 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            new SnowFlake(128, 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            new SnowFlake(0, -1)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            new SnowFlake(0, 128)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def snowFlake = new SnowFlake(1, 1)
        snowFlake.lastStamp = System.currentTimeMillis() + 1000
        try {
            snowFlake.nextId()
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
