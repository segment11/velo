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

        expect:
        keyAnalysisHandler.topKPrefixCounts.get().size() == 0

        when:
        10.times {
            keyAnalysisHandler.addKey('key:' + (it.toString().padLeft(12, '0')), 10)
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

        cleanup:
        keyAnalysisHandler.cleanUp()
        Thread.sleep(2000)
        eventloop.breakEventloop()
    }
}
