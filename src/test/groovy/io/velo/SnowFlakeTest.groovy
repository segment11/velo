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

    def 'test expire'() {
        given:
        def snowFlake = new SnowFlake(1, 1)

        when:
        def seq = snowFlake.nextId()
        then:
        !SnowFlake.isExpired(seq, 0)
        !SnowFlake.isExpired(seq, 1)
        !SnowFlake.isExpired(seq, 1, System.currentTimeMillis())

        when:
        Thread.sleep(1000 * 2)
        then:
        SnowFlake.isExpired(seq, 1)
    }

    def 'test as begin scan seq epoch'() {
        given:
        def snowFlakeA = new SnowFlake(1, 1)
        def snowFlakeB = new SnowFlake(2, 2)

        when:
        def idBefore = snowFlakeA.nextId()
        Thread.sleep(2)
        def epoch = SnowFlake.asBeginScanSeqEpoch()
        Thread.sleep(2)
        def idAfter = snowFlakeB.nextId()
        then:
        // timestamp part only, no datacenter / machine / sequence bits
        (epoch & ((1L << SnowFlake.TIMESTAMP_LEFT) - 1)) == 0L
        epoch > 0L
        // ids minted in an earlier millisecond are older than the epoch
        idBefore < epoch
        // an id from an independent instance minted later is newer, instance bits do not affect the order
        idAfter > epoch

        when:
        Thread.sleep(2)
        then:
        SnowFlake.asBeginScanSeqEpoch() > epoch
    }
}
