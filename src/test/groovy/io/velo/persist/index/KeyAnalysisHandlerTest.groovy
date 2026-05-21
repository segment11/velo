package io.velo.persist.index

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.ConfForGlobal
import io.velo.persist.Consts
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class KeyAnalysisHandlerTest extends Specification {
    def 'test addCount only increments for new keys'() {
        given:
        def keyDir = new File(Consts.persistDir, 'keys-for-analysis')
        if (!keyDir.exists()) {
            keyDir.mkdirs()
        }

        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        // 100% sampling, small budget
        ConfForGlobal.keyAnalysisNumberPercent = 100
        ConfForGlobal.estimateKeyNumber = 5
        def keyAnalysisHandler = new KeyAnalysisHandler(keyDir, eventloop, Config.create())

        when: 'write the same key 10 times'
        int valueLen = 10 << 8
        10.times {
            keyAnalysisHandler.addKey('hot:key', valueLen)
            Thread.sleep(20)
        }
        Thread.sleep(500)

        and: 'write a different new key'
        keyAnalysisHandler.addKey('new:key', valueLen)
        Thread.sleep(500)

        and:
        def metrics = KeyAnalysisHandler.keyAnalysisGauge.collect()
        def samples = metrics[0].samples

        then: 'addCount should be 2 (2 unique keys), not 11 (10 rewrites + 1 new)'
        samples.find { it.name == 'key_analysis_add_count' }.value == 2
        !keyAnalysisHandler.isKeyAnalysisNumberFull

        when: 'close and reopen DB to verify addValueLengthTotal is restored'
        def addValueLengthTotalBefore = keyAnalysisHandler.addValueLengthTotal
        keyAnalysisHandler.cleanUp()
        Thread.sleep(500)

        def eventloop2 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop2.keepAlive(true)
        Thread.start {
            eventloop2.run()
        }
        def handler2 = new KeyAnalysisHandler(keyDir, eventloop2, Config.create())

        then:
        handler2.addCount == 2
        handler2.addValueLengthTotal == addValueLengthTotalBefore
        handler2.addValueLengthTotal == 20 // 2 keys * 10 bytes each

        cleanup:
        handler2.flushdb()
        Thread.sleep(1000)
        handler2.cleanUp()
        Thread.sleep(1000)
        KeyAnalysisHandler.keyAnalysisGauge.clearRawGetterList()
        eventloop2.breakEventloop()
        eventloop.breakEventloop()
    }

    def 'test all'() {
        given:
        def keyDir = new File(Consts.persistDir, 'keys-for-analysis')
        if (!keyDir.exists()) {
            keyDir.mkdirs()
        }

        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        ConfForGlobal.keyAnalysisNumberPercent = 50
        ConfForGlobal.estimateKeyNumber = 20
        def keyAnalysisHandler = new KeyAnalysisHandler(keyDir, eventloop, Config.create())
        def innerTask = keyAnalysisHandler.innerTask

        expect:
        keyAnalysisHandler.topKPrefixCounts.get().size() == 0

        when:
        keyAnalysisHandler.resetInnerTask(Config.create())
        keyAnalysisHandler.run()
        then:
        keyAnalysisHandler.innerTask != innerTask

        when:
        int valueBytesInit = 10 << 8
        10.times {
            def kk = 'key:' + (it.toString().padLeft(12, '0'))
            keyAnalysisHandler.addKey(kk, valueBytesInit)
            Thread.sleep(10)
            keyAnalysisHandler.addKey(kk, valueBytesInit)
            Thread.sleep(10)
        }
        // full
        def kk11 = 'key:000000000011'
        keyAnalysisHandler.addKey(kk11, valueBytesInit)

        keyAnalysisHandler.removeKey('key:000000000000')
        keyAnalysisHandler.removeKey('key:000000000001')
        Thread.sleep(2000)
        def metrics = KeyAnalysisHandler.keyAnalysisGauge.collect()
        def samples = metrics[0].samples
        then:
        samples.size() == 3
        samples.find { it.name == 'key_analysis_add_count' }.value == 10
        samples.find { it.name == 'key_analysis_all_key_count' }.value == 8

        when:
        int iterateKeyCount = 0
        def f = keyAnalysisHandler.iterateKeys(null, 10, true, { bb, valueLength ->
            println new String(bb) + ': ' + valueLength
            iterateKeyCount++
        })
        f.get()
        then:
        iterateKeyCount == 8

        when:
        def pattern2 = ~/key:.+/
        def f2 = keyAnalysisHandler.prefixMatch('key:', pattern2, 10)
        def keyList = f2.get()
        then:
        keyList.size() == 8

        when:
        def f3 = keyAnalysisHandler.prefixMatch('key:', pattern2, 5)
        def keyList3 = f3.get()
        then:
        keyList3.size() == 5

        when:
        def f4 = keyAnalysisHandler.prefixMatch('key:000000000002', pattern2, 5)
        def keyList4 = f4.get()
        then:
        keyList4.size() == 1

        // test filter keys
        when:
        def f5 = keyAnalysisHandler.filterKeys(null, 5,
                key -> {
                    true
                }, valueBytesAsInt -> {
            true
        })
        def keyList5 = f5.get()
        then:
        keyList5.size() == 5

        when:
        // exclude
        def f6 = keyAnalysisHandler.filterKeys('key:000000000008'.bytes, 5,
                key -> {
                    true
                }, valueBytesAsInt -> {
            true
        })
        def keyList6 = f6.get()
        then:
        keyList6.size() == 1

        when:
        def bytes = new byte[2 + 4 + 4 + 2 + 4 + 4]
        def buffer = ByteBuffer.wrap(bytes)
        buffer.putShort((short) 4)
        buffer.put('1234'.bytes)
        buffer.putInt(1)
        buffer.putShort((short) 4)
        buffer.put('4321'.bytes)
        buffer.putInt(1)
        def f7 = keyAnalysisHandler.addBatch(bytes)
        def resultF7 = f7.get()
        then:
        resultF7.keyCount() == 2
        resultF7.lastKeyBytes() == '4321'.bytes

        when:
        def f8 = keyAnalysisHandler.flushdb()
        f8.get()
        f5 = keyAnalysisHandler.filterKeys(null, 5,
                key -> {
                    true
                }, valueBytesAsInt -> {
            true
        })
        keyList5 = f5.get()
        then:
        keyList5.size() == 0

        cleanup:
        keyAnalysisHandler.flushdb()
        Thread.sleep(1000)
        keyAnalysisHandler.cleanUp()
        Thread.sleep(1000)
        eventloop.breakEventloop()
    }
}
