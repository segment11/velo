package io.velo.type

import com.google.common.hash.BloomFilter
import com.google.common.hash.Funnels
import spock.lang.Specification

import java.nio.charset.Charset

class BFSerializerTest extends Specification {
    def 'test all'() {
        given:
        def filter = BloomFilter.create(Funnels.stringFunnel(Charset.defaultCharset()), 10000, 0.01)

        when:
        10.times {
            def item = 'item:' + it
            filter.put(item)
        }
        def bytes = BFSerializer.toBytes(filter)
        println 'bloom filter serialize bytes length: ' + bytes.length
        def filter2 = BFSerializer.fromBytes(bytes, 0, bytes.length)
        then:
        (0..<10).every {
            def item = 'item:' + it
            filter2.mightContain(item)
        }
    }
}
