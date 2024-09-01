package io.velo.monitor

import spock.lang.Specification

class BigKeyTopKTest extends Specification {
    def 'test top k'() {
        given:
        def topK = new BigKeyTopK(3)

        def one = new BigKeyTopK.BigKey('xxx'.bytes, 1)
        def two = new BigKeyTopK.BigKey('yyy'.bytes, 1)
        println one
        println two

        expect:
        one.equals(one)
        !one.equals(two)

        when:
        for (i in 0..<10) {
            topK.add(('key:' + i).bytes, i)
        }
        println topK.queue.first()
        println topK.queue.last()
        then:
        topK.size() == 3
        topK.queue.first().keyBytes() == 'key:7'.bytes
        topK.sizeIfBiggerThan(8) == 2

        when:
        // already contains
        for (i in 0..<10) {
            topK.add(('key:' + i).bytes, i)
        }
        then:
        topK.sizeIfBiggerThan(8) == 2
    }
}
