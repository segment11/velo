package io.velo.persist

import io.velo.ConfForGlobal
import io.velo.ConfForSlot
import spock.lang.Specification

import java.nio.ByteBuffer

import static Consts.getSlotDir

class StatKeyCountInBucketsTest extends Specification {
    final short slot = 0

    def 'test for repl'() {
        given:
        def one = new StatKeyCountInBuckets(slot, slotDir)
        def two = new StatKeyCountInBuckets(slot, slotDir)
        println 'in memory size estimate: ' + one.estimate()

        when:
        def allInMemoryCachedBytes = one.getInMemoryCachedBytes()
        then:
        allInMemoryCachedBytes.length == one.allCapacity

        when:
        def bytes0 = new byte[one.allCapacity]
        def buffer0 = ByteBuffer.wrap(bytes0)
        ConfForSlot.global.confBucket.bucketsPerSlot.times {
            buffer0.putShort((short) 1)
        }
        one.overwriteInMemoryCachedBytes(bytes0)
        then:
        one.inMemoryCachedBytes.length == one.allCapacity
        one.keyCount == one.allCapacity / 2

        when:
        ConfForGlobal.pureMemory = true
        one.overwriteInMemoryCachedBytes(bytes0)
        then:
        one.inMemoryCachedBytes.length == one.allCapacity
        one.keyCount == one.allCapacity / 2

        when:
        boolean exception = false
        def bytes0WrongSize = new byte[one.allCapacity - 1]
        try {
            one.overwriteInMemoryCachedBytes(bytes0WrongSize)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

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
        def one1 = new StatKeyCountInBuckets(slot, slotDir)
        then:
        one1.getKeyCountForBucketIndex(10) == 10
        one1.getKeyCountForBucketIndex(20) == 20
        one1.keyCount == 30

        when:
        ConfForGlobal.pureMemory = true
        def one2 = new StatKeyCountInBuckets(slot, slotDir)
        one2.setKeyCountBatch(0, 0, keyCountArray)
        then:
        one2.getKeyCountForBucketIndex(10) == 10
        one2.getKeyCountForBucketIndex(20) == 20
        one2.keyCount == 30

        when:
        keyCountArray[10] = (short) 20
        keyCountArray[20] = (short) 40
        one2.setKeyCountBatch(0, 0, keyCountArray)
        then:
        one2.getKeyCountForBucketIndex(10) == 20
        one2.getKeyCountForBucketIndex(20) == 40
        one2.keyCount == 60

        cleanup:
        ConfForGlobal.pureMemory = false
        one.clear()
        one.cleanUp()
        one1.cleanUp()
        ConfForGlobal.pureMemory = true
        one2.clear()
        one2.cleanUp()
        ConfForGlobal.pureMemory = false
        slotDir.deleteDir()
    }
}
