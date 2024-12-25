package io.velo.persist

import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.ConfForSlot
import io.velo.KeyHash
import spock.lang.Specification

class AllKeyHashBucketsTest extends Specification {
    def 'test set and get'() {
        given:
        ConfForGlobal.pureMemoryV2 = true
        def allKeyHashBuckets = new AllKeyHashBuckets(65536)
        def x = new AllKeyHashBuckets.RecordX(0L, 0L, (byte) 0, 0L)
        println x.toPvm()

        when:
        def n = 10000 * 100
        def sList = (0..<n).collect {
            def key = 'key:' + (it.toString().padLeft(12, '0'))
            def keyHash32 = KeyHash.hash32(key.bytes)
            def s = BaseCommand.slot(key.bytes, 1)
            new Tuple2<BaseCommand.SlotWithKeyHash, Integer>(s, keyHash32)
        }
        sList.eachWithIndex { s, i ->
            allKeyHashBuckets.put(s.v2, s.v1.bucketIndex(), 0L, i, (byte) 0, i)
        }
        boolean isMatchAll = true
        sList.eachWithIndex { s, i ->
            def isMatch = allKeyHashBuckets.get(s.v2, s.v1.bucketIndex()).recordId() == i
            isMatchAll &= isMatch
        }
        then:
        isMatchAll

        when:
        sList[0..<10].each {
            allKeyHashBuckets.remove(it.v2, it.v1.bucketIndex())
        }
        then:
        sList[0..<10].every {
            allKeyHashBuckets.get(it.v2, it.v1.bucketIndex()) == null
        }
        sList[0..<10].every {
            !allKeyHashBuckets.remove(it.v2, it.v1.bucketIndex())
        }

        when:
        boolean exception = false
        try {
            allKeyHashBuckets.put(0, 0, AllKeyHashBuckets.MAX_EXPIRE_AT + 1, 0L, (byte) 0, 0L)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def sb = new StringBuilder()
        allKeyHashBuckets.estimate(sb)
        println sb.toString()
        then:
        1 == 1

        when:
        allKeyHashBuckets.putLocalValue(1L, new byte[10])
        then:
        allKeyHashBuckets.getLocalValue(1L).length == 10

        cleanup:
        ConfForGlobal.pureMemoryV2 = false
    }

    def 'test init'() {
        given:
        ConfForGlobal.pureMemoryV2 = true
        def sb = new StringBuilder()

        when:
        def a1m = new AllKeyHashBuckets(65536)
        then:
        a1m.estimate(sb) == 128L * 64 * 1024 * 7

        when:
        ConfForSlot.global = ConfForSlot.c10m
        def a10m = new AllKeyHashBuckets(256 * 1024)
        then:
        a10m.estimate(sb) == 256L * 256 * 1024 * 7

        when:
        println sb.toString()
        then:
        1 == 1

        cleanup:
        ConfForGlobal.pureMemoryV2 = false
        a1m.cleanUp()
        a10m.cleanUp()
    }

    def 'test pvm to record id'() {
        given:
        def pvm = new PersistValueMeta()
        pvm.segmentIndex = 512 * 1024 * 64 - 1
        pvm.subBlockIndex = 3
        pvm.segmentOffset = 64 * 1024 - 1
        println pvm

        when:
        def recordId2 = AllKeyHashBuckets.pvmToRecordId(pvm)
        def recordId3 = AllKeyHashBuckets.positionToRecordId(1, (byte) 1, 1)
        def pvm2 = AllKeyHashBuckets.recordIdToPvm(recordId2)
        def pvm3 = AllKeyHashBuckets.recordIdToPvm(recordId3)
        println pvm2
        println pvm3
        then:
        pvm2.segmentIndex == pvm.segmentIndex
        pvm2.subBlockIndex == pvm.subBlockIndex
        pvm2.segmentOffset == pvm.segmentOffset
        pvm3.segmentIndex == 1
        pvm3.subBlockIndex == (byte) 1
        pvm3.segmentOffset == 1
    }

    def 'test save and load'() {
        given:
        ConfForGlobal.pureMemoryV2 = true
        def allKeyHashBuckets = new AllKeyHashBuckets(65536)
        def allKeyHashBuckets2 = new AllKeyHashBuckets(65536)

        and:
        10.times {
            allKeyHashBuckets.put(it + 1, it, 10L, it + 1, (byte) 0, it + 1)
        }

        expect:
        allKeyHashBuckets.getKeyCountInBucketIndex(0) == 1
        (0..<10).every {
            allKeyHashBuckets.put(it + 1, it, 10L, it + 1, (byte) 0, it + 1)
        }

        when:
        def bos = new ByteArrayOutputStream()
        def os = new DataOutputStream(bos)
        allKeyHashBuckets.writeToSavedFileWhenPureMemory(os)
        def bis = new ByteArrayInputStream(bos.toByteArray())
        def is = new DataInputStream(bis)
        allKeyHashBuckets2.loadFromLastSavedFileWhenPureMemory(is)
        then:
        (0..<10).every {
            allKeyHashBuckets2.get(it + 1, it).expireAt() == 10L
        }

        cleanup:
        ConfForGlobal.pureMemoryV2 = false
    }

    def 'test repl'() {
        given:
        def allKeyHashBuckets = new AllKeyHashBuckets(65536)

        when:
        def bb = allKeyHashBuckets.getRecordsBytesArrayByWalGroupIndex(0)
        then:
        bb.length == ConfForSlot.global.confWal.oneChargeBucketNumber
        bb[0].length == 472
    }
}
