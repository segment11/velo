package io.velo.persist

import io.velo.ConfForGlobal
import spock.lang.Specification

import static Consts.getSlotDir

class MetaKeyBucketSplitNumberTest extends Specification {
    final short slot = 0

    def 'test for repl'() {
        given:
        def one = new MetaKeyBucketSplitNumber(slot, slotDir)
        println 'in memory size estimate: ' + one.estimate()

        when:
        def allInMemoryCachedBytes = one.getInMemoryCachedBytes()
        then:
        allInMemoryCachedBytes.length == one.allCapacity

        when:
        def bytes0 = new byte[one.allCapacity]
        one.overwriteInMemoryCachedBytes(bytes0)
        then:
        one.inMemoryCachedBytes.length == one.allCapacity

        when:
        ConfForGlobal.pureMemory = true
        one.overwriteInMemoryCachedBytes(bytes0)
        then:
        one.inMemoryCachedBytes.length == one.allCapacity

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

        when:
        def two = new MetaKeyBucketSplitNumber(slot, slotDir)
        then:
        two != null

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

        when:
        ConfForGlobal.pureMemory = true
        def one2 = new MetaKeyBucketSplitNumber(slot, slotDir)
        one2.set(10, (byte) 3)
        one2.set(20, (byte) 9)
        one2.set(30, (byte) 27)
        then:
        one2.get((byte) 10) == 3
        one2.get((byte) 20) == 9
        one2.get((byte) 30) == 27

        when:
        byte[] splitNumberArray2 = [3, 9, 27]
        one2.setBatch(10, splitNumberArray2)
        then:
        one2.get((byte) 10) == 3
        one2.get((byte) 11) == 9
        one2.get((byte) 12) == 27
        one2.getBatch(10, 3) == splitNumberArray2

        when:
        def maxSplitNumber = one2.maxSplitNumber()
        then:
        maxSplitNumber == 27

        cleanup:
        ConfForGlobal.pureMemory = false
        one.clear()
        one.cleanUp()
        ConfForGlobal.pureMemory = true
        one2.clear()
        one2.cleanUp()
        ConfForGlobal.pureMemory = false
        slotDir.deleteDir()
    }
}
