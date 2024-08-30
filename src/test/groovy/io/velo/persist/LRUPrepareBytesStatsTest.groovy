package io.velo.persist


import spock.lang.Specification

class LRUPrepareBytesStatsTest extends Specification {
    def 'test all'() {
        given:
        LRUPrepareBytesStats.list.clear()

        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_key_bucket, 'key-bucket-0', 1024 * 1024, true)
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.fd_chunk_data, 'chunk-data-0', 1024 * 1024, true)

        expect:
        LRUPrepareBytesStats.list.size() == 2
        LRUPrepareBytesStats.sum() == 1024 * 1024 * 2
        LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.fd_key_bucket) == 1024 * 1024
        LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.fd_chunk_data) == 1024 * 1024
        LRUPrepareBytesStats.sum(LRUPrepareBytesStats.Type.big_string) == 0

        when:
        LRUPrepareBytesStats.removeOne(LRUPrepareBytesStats.Type.fd_key_bucket, 'key-bucket-0')
        then:
        LRUPrepareBytesStats.list.size() == 1
    }
}
