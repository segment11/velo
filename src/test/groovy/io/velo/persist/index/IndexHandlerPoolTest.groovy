package io.velo.persist.index

import io.activej.config.Config
import io.velo.persist.Consts
import spock.lang.Specification

class IndexHandlerPoolTest extends Specification {
    def 'test start and clean up'() {
        given:
        def pool = new IndexHandlerPool((byte) 2, Consts.persistDir, Config.create())

        when:
        pool.start()
        then:
        pool.keyAnalysisHandler != null
        pool.indexHandlers.length == 2
        pool.getIndexHandler((byte) 0) != null
        pool.indexHandlers[0].threadIdProtectedForSafe != 0

        when:
        Thread.sleep(200)
        pool.indexHandlers[0].threadIdProtectedForSafe = Thread.currentThread().threadId()
        pool.run((byte) 0) {
            println 'async run'
        }
        then:
        1 == 1
    }
}
