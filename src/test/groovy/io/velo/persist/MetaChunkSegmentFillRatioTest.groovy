package io.velo.persist

import io.velo.ConfForGlobal
import spock.lang.Specification

class MetaChunkSegmentFillRatioTest extends Specification {
    def 'test base'() {
        given:
        ConfForGlobal.pureMemory = true
        ConfForGlobal.pureMemoryV2 = true

        def one = new MetaChunkSegmentFillRatio()

        when:
        one.set(0, 10000)
        one.set(0, 20000)
        one.remove(0, 1000)
        then:
        one.findSegmentsFillRatioLessThan(0, 10, 20) == [0]
        one.fillRatioBucketSegmentCount[19] == 1

        when:
        one.estimate(new StringBuilder())
        then:
        1 == 1

        when:
        one.flush()
        then:
        one.findSegmentsFillRatioLessThan(0, 10, 20).isEmpty()
        one.fillRatioBucketSegmentCount[19] == 0

        cleanup:
        ConfForGlobal.pureMemory = false
        ConfForGlobal.pureMemoryV2 = false
    }

    def 'test save and load'() {
        given:
        ConfForGlobal.pureMemory = true
        ConfForGlobal.pureMemoryV2 = true

        def one = new MetaChunkSegmentFillRatio()

        when:
        one.set(0, 10000)
        def bos = new ByteArrayOutputStream()
        def os = new DataOutputStream(bos)
        one.writeToSavedFileWhenPureMemory(os)
        def bis = new ByteArrayInputStream(bos.toByteArray())
        def is = new DataInputStream(bis)
        one.loadFromLastSavedFileWhenPureMemory(is)
        then:
        one.findSegmentsFillRatioLessThan(0, 10, 21) == [0]
        one.fillRatioBucketSegmentCount[20] == 1

        cleanup:
        ConfForGlobal.pureMemory = false
        ConfForGlobal.pureMemoryV2 = false
    }

}
