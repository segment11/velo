package io.velo.repl.cluster

import spock.lang.Specification

class MultiShardShadowTest extends Specification {
    def 'test all'() {
        given:
        def m = new MultiShardShadow()

        when:
        def shards = new Shard[2]
        var shard0 = new Shard()
        var shard1 = new Shard()
        shards[0] = shard0
        shards[1] = shard1
        shard0.multiSlotRange.addSingle(0, 8191)
        shard1.multiSlotRange.addSingle(8192, 16383)
        m.shards = shards
        m.mySelfShard = shard0
        then:
        m.mySelfShard == shard0
        m.getShardBySlot(0) == shard0
        m.getShardBySlot(8192) == shard1
        m.getShardBySlot(16384) == null
    }
}
