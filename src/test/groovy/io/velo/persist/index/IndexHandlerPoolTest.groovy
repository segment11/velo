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
        pool.indexHandlers.length == 2
        pool.indexHandlers[0].threadIdProtectedForSafe != 0
        pool.getChargeWorkerIdByWordKeyHash(1234L) == (byte) 0
        pool.getChargeWorkerIdByWordKeyHash(123L) == (byte) 1

        when:
        Thread.sleep(200)
        pool.indexHandlers[0].threadIdProtectedForSafe = Thread.currentThread().threadId()
        pool.run((byte) 0) {
            println 'async run'
        }
        then:
        1 == 1

        when:
        pool.cleanUp()
        def pool2 = new IndexHandlerPool((byte) 1, Consts.persistDir, Config.create())
        then:
        pool2.getChargeWorkerIdByWordKeyHash(123L) == (byte) 0
        // skip clean up as not started
    }
}
