package io.velo.persist

import io.velo.ConfForGlobal
import spock.lang.Specification

class MetaChunkSegmentFillRatioTest extends Specification {
    def 'test base'() {
        given:
        ConfForGlobal.pureMemory = true
        ConfForGlobal.pureMemoryV2 = true

        def one = new MetaChunkSegmentFillRatio((short) 0)

        when:
        one.set(0, 10000)
        one.remove(0, 1000)
        then:
        0 in one.fillRatioBucketArray[18]

        when:
        one.estimate(new StringBuilder())
        then:
        1 == 1

        when:
        one.flush()
        then:
        0 !in one.fillRatioBucketArray[18]

        cleanup:
        ConfForGlobal.pureMemory = false
        ConfForGlobal.pureMemoryV2 = false
    }

    def 'test save and load'() {
        given:
        ConfForGlobal.pureMemory = true
        ConfForGlobal.pureMemoryV2 = true

        def one = new MetaChunkSegmentFillRatio((short) 0)

        when:
        one.set(0, 10000)
        def bos = new ByteArrayOutputStream()
        def os = new DataOutputStream(bos)
        one.writeToSavedFileWhenPureMemory(os)
        def bis = new ByteArrayInputStream(bos.toByteArray())
        def is = new DataInputStream(bis)
        one.loadFromLastSavedFileWhenPureMemory(is)
        then:
        0 in one.fillRatioBucketArray[20]

        cleanup:
        ConfForGlobal.pureMemory = false
        ConfForGlobal.pureMemoryV2 = false
    }

}
