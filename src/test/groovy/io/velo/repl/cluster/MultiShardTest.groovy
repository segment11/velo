package io.velo.repl.cluster

import io.velo.ConfForGlobal
import io.velo.persist.Consts
import spock.lang.Specification

class MultiShardTest extends Specification {
    def 'test all'() {
        given:
        Consts.persistDir.mkdirs()
        ConfForGlobal.netListenAddresses = 'localhost:7379'
        def multiShard = new MultiShard(Consts.persistDir)

        expect:
        MultiShard.isToClientSlotSkip(1)
        !MultiShard.isToClientSlotSkip(0)
        MultiShard.asInnerSlotByToClientSlot(0) == 0
        MultiShard.asInnerSlotByToClientSlot(1) == 0

        when:
        multiShard.shards << new Shard(nodes: [new Node(master: true, host: 'localhost', port: 7380)])
        multiShard.saveMeta()
        multiShard.updateClusterVersion(0)
        multiShard.updateClusterVersion(1)
        then:
        multiShard.clusterCurrentEpoch == 1

        when:
        def multiShard2 = new MultiShard(Consts.persistDir)
        then:
        multiShard2.shards.size() == 2

        when:
        multiShard2.refreshAllShards(multiShard.shards, 1)
        then:
        multiShard2.shards.size() == 2
        multiShard2.clusterCurrentEpoch == 1
        multiShard2.clusterMyEpoch > 0

        cleanup:
        Consts.persistDir.deleteDir()
    }
}
