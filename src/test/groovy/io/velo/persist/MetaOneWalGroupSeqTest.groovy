package io.velo.persist

import io.velo.ConfForGlobal
import spock.lang.Specification

import static Consts.getSlotDir

class MetaOneWalGroupSeqTest extends Specification {
    final short slot = 0

    def 'test set and get'() {
        given:
        ConfForGlobal.pureMemory = false

        def one = new MetaOneWalGroupSeq(slot, slotDir)
        println 'in memory size estimate: ' + one.estimate(new StringBuilder())

        when:
        one.set(0, (byte) 0, 1L)
        one.set(1, (byte) 0, 1L)
        then:
        one.get(0, (byte) 0) == 1L
        one.get(1, (byte) 0) == 1L

        when:
        one.cleanUp()
        def one2 = new MetaOneWalGroupSeq(slot, slotDir)
        then:
        one2.get(0, (byte) 0) == 1L

        when:
        one2.overwriteInMemoryCachedBytes(new byte[one2.allCapacity])
        then:
        one2.get(0, (byte) 0) == 0L
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

        when:
        one2.clear()
        one2.cleanUp()
        ConfForGlobal.pureMemory = true
        def one3 = new MetaOneWalGroupSeq(slot, slotDir)
        one3.set(0, (byte) 0, 1L)
        then:
        one3.get(0, (byte) 0) == 1L

        cleanup:
        one3.clear()
        one3.cleanUp()
        ConfForGlobal.pureMemory = false
        slotDir.deleteDir()
    }
}
