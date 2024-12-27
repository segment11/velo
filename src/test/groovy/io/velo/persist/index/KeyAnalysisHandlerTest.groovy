package io.velo.persist.index

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.persist.Consts
import spock.lang.Specification

import java.time.Duration

class KeyAnalysisHandlerTest extends Specification {
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
            keyAnalysisHandler.addKey('key:' + (it.toString().padLeft(12, '0')), valueBytesInit)
        }
        keyAnalysisHandler.removeKey('key:000000000000')
        keyAnalysisHandler.removeKey('key:000000000001')
        Thread.sleep(2000)
        def metrics = KeyAnalysisHandler.keyAnalysisGauge.collect()
        def samples = metrics[0].samples
        then:
        samples.size() == 3
        samples.find { it.name == 'key_analysis_add_count' }.value == 10
        samples.find { it.name == 'key_analysis_remove_or_expire_count' }.value == 2
        samples.find { it.name == 'key_analysis_add_value_length_avg' }.value == 10

        when:
        int iterateKeyCount = 0
        def f = keyAnalysisHandler.iterateKeys(null, 10, { bb, valueLength ->
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
        def f6 = keyAnalysisHandler.filterKeys('key:000000000008'.bytes, 5,
                key -> {
                    true
                }, valueBytesAsInt -> {
            true
        })
        def keyList6 = f6.get()
        then:
        keyList6.size() == 2

        cleanup:
        keyAnalysisHandler.cleanUp()
        Thread.sleep(2000)
        eventloop.breakEventloop()
    }
}
