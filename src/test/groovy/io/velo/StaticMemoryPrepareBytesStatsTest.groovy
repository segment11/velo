package io.velo


import spock.lang.Specification

class StaticMemoryPrepareBytesStatsTest extends Specification {
    def 'test all'() {
        given:
        StaticMemoryPrepareBytesStats.list.clear()

        StaticMemoryPrepareBytesStats.add(StaticMemoryPrepareBytesStats.Type.wal_cache, 1024 * 1024, true)

        expect:
        StaticMemoryPrepareBytesStats.list.size() == 1
        StaticMemoryPrepareBytesStats.sum() == 1024 * 1024
        StaticMemoryPrepareBytesStats.sum(StaticMemoryPrepareBytesStats.Type.wal_cache) == 1024 * 1024
    }
}
