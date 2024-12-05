package io.velo.persist.index

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

        def keyAnalysisHandler = new KeyAnalysisHandler(keyDir, eventloop)

        when:
        10.times {
            keyAnalysisHandler.addKey('key:' + (it.toString().padLeft(12, '0')))
        }
        keyAnalysisHandler.removeKey('key:000000000000')
        keyAnalysisHandler.removeKey('key:000000000001')
        Thread.sleep(100)
        def metricsMap = keyAnalysisHandler.collect()
        then:
        metricsMap['key_analysis_add_count'] == 10
        metricsMap['key_analysis_remove_count'] == 2

        when:
        int iterateKeyCount = 0
        def f = keyAnalysisHandler.iterateKeys(null, 10, { bb ->
            println new String(bb)
            iterateKeyCount++
        })
        f.get()
        then:
        iterateKeyCount == 8

        cleanup:
        keyAnalysisHandler.cleanUp()
        eventloop.breakEventloop()
    }
}
