package io.velo.persist

import io.velo.repl.Binlog
import spock.lang.Specification

import static Consts.getSlotDir

class MetaChunkSegmentIndexTest extends Specification {
    final short slot = 0

    def 'test set and get'() {
        given:
        def one = new MetaChunkSegmentIndex(slot, slotDir)

        when:
        one.set(10)
        then:
        one.get() == 10

        when:
        one.set(20)
        then:
        one.get() == 20

        when:
        one.setMasterBinlogFileIndexAndOffset(10L, true, 1, 0L)
        then:
        one.masterBinlogFileIndexAndOffset == new Binlog.FileIndexAndOffset(1, 0L)

        when:
        one.clearMasterBinlogFileIndexAndOffset()
        then:
        one.masterBinlogFileIndexAndOffset == new Binlog.FileIndexAndOffset(0, 0L)

        when:
        one.setAll(30, 1L, false, 2, 0)
        then:
        one.get() == 30
        one.masterUuid == 1L
        !one.isExistsDataAllFetched()
        one.masterBinlogFileIndexAndOffset == new Binlog.FileIndexAndOffset(2, 0)

        when:
        one.setAll(30, 1L, true, 2, 0)
        then:
        one.isExistsDataAllFetched()

        when:
        one.clear()
        then:
        one.get() == 0

        cleanup:
        one.cleanUp()
        slotDir.deleteDir()
    }
}
