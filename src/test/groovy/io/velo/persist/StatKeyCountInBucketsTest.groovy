package io.velo.persist

import spock.lang.Specification

import static Consts.getSlotDir

class StatKeyCountInBucketsTest extends Specification {
    final short slot = 0

    def 'test set and get'() {
        given:
        def one = new StatKeyCountInBuckets(slot, slotDir)

        when:
        short[] keyCountArray = new short[32]
        keyCountArray[10] = (short) 10
        keyCountArray[20] = (short) 20
        one.setKeyCountBatch(0, 0, keyCountArray)
        then:
        one.getKeyCountForBucketIndex(10) == 10
        one.getKeyCountForBucketIndex(20) == 20
        one.getKeyCountForOneWalGroup(0) == 30
        one.keyCount == 30

        when:
        one.setKeyCountForBucketIndex(10, (short) 11)
        then:
        one.getKeyCountForBucketIndex(10) == 11

        when:
        def one1 = new StatKeyCountInBuckets(slot, slotDir)
        then:
        one1.getKeyCountForBucketIndex(10) == 10
        one1.getKeyCountForBucketIndex(20) == 20
        one1.keyCount == 30

        cleanup:
        one.clear()
        one.cleanUp()
        one1.cleanUp()
        slotDir.deleteDir()
    }
}
