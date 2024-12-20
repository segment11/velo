package io.velo.persist

import io.velo.ConfForGlobal
import io.velo.repl.Binlog
import spock.lang.Specification

import static Consts.getSlotDir

class MetaChunkSegmentIndexTest extends Specification {
    final short slot = 0

    def 'test for repl'() {
        given:
        def one = new MetaChunkSegmentIndex(slot, slotDir)
        def one2 = new MetaChunkSegmentIndex(slot, slotDir)

        when:
        one2.cleanUp()
        ConfForGlobal.pureMemory = true
        def two = new MetaChunkSegmentIndex(slot, slotDir)
        then:
        two.get() == one.get()

        cleanup:
        one.clear()
        one.cleanUp()
        two.cleanUp()
        ConfForGlobal.pureMemory = false
        slotDir.deleteDir()
    }

    def 'test set and get'() {
        given:
        ConfForGlobal.pureMemory = false
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

        when:
        ConfForGlobal.pureMemory = true
        def one2 = new MetaChunkSegmentIndex(slot, slotDir)
        one2.set(10)
        then:
        one2.get() == 10

        when:
        one2.setAll(10, 1L, true, 0, 0)
        then:
        one2.isExistsDataAllFetched()

        when:
        one2.overwriteInMemoryCachedBytes(new byte[one2.allCapacity])
        then:
        one2.get() == 0
        one2.getInMemoryCachedBytes().length == one2.allCapacity

        when:
        boolean exception = false
        try {
            one2.overwriteInMemoryCachedBytes(new byte[one2.allCapacity + 1])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        ConfForGlobal.pureMemory = false
        one.cleanUp()
        ConfForGlobal.pureMemory = true
        one2.cleanUp()
        ConfForGlobal.pureMemory = false
        slotDir.deleteDir()
    }
}
