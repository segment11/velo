package io.velo.monitor

import spock.lang.Specification

class BigKeyTopKTest extends Specification {
    def 'test top k'() {
        given:
        def topK = new BigKeyTopK(3)

        when:
        for (i in 0..<10) {
            topK.add(('key:' + i).bytes, i)
        }
        println topK.queue.first()
        println topK.queue.last()
        then:
        topK.size() == 3
        topK.queue.first().keyBytes() == 'key:9'.bytes

        when:
        // add again, all skip
        for (i in 0..<10) {
            topK.add(('key:' + i).bytes, i)
        }
        then:
        topK.queue.first().keyBytes() == 'key:9'.bytes
    }
}
