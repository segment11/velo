package io.velo.persist

import spock.lang.Specification

import static Consts.getSlotDir

class MetaKeyBucketSplitNumberTest extends Specification {
    final short slot = 0

    def 'test set and get'() {
        given:
        def one = new MetaKeyBucketSplitNumber(slot, slotDir)
//        println one.inMemoryCachedBytes

        when:
        one.set(10, (byte) 3)
        one.set(20, (byte) 9)
        one.set(30, (byte) 27)
        then:
        one.get((byte) 10) == 3
        one.get((byte) 20) == 9
        one.get((byte) 30) == 27

        when:
        byte[] splitNumberArray = [3, 9, 27]
        one.setBatch(10, splitNumberArray)
        then:
        one.get((byte) 10) == 3
        one.get((byte) 11) == 9
        one.get((byte) 12) == 27
        one.getBatch(10, 3) == splitNumberArray

        cleanup:
        one.clear()
        one.cleanUp()
        slotDir.deleteDir()
    }
}
