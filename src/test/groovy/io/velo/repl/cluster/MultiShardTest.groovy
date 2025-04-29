package io.velo.repl.cluster

import io.velo.ConfForGlobal
import io.velo.RequestHandler
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import spock.lang.Specification

class MultiShardTest extends Specification {
    def 'test all'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance

        and:
        ConfForGlobal.netListenAddresses = 'localhost:7379'
        RequestHandler.initMultiShardShadows((byte) 1)
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
        multiShard.shards[0].nodes[0].mySelf = false
        multiShard.shards[1].nodes[0].mySelf = true
        then:
        multiShard.mySelfShard() == multiShard.shards[1]

        when:
        multiShard.shards[1].nodes[0].mySelf = false
        then:
        multiShard.mySelfShard() == null

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

        when:
        multiShard.shards.clear()
        multiShard.shards << new Shard(nodes: [new Node(master: true, host: 'localhost', port: 7380)])
        then:
        multiShard.firstToClientSlot() == null
        multiShard.lastToClientSlot() == null
        multiShard.nextToClientSlot(0) == null

        when:
        multiShard.shards[0].multiSlotRange.addSingle(1024, 2047)
        then:
        multiShard.firstToClientSlot() == 1024
        multiShard.lastToClientSlot() == 2047
        multiShard.nextToClientSlot(0) == 1024
        multiShard.nextToClientSlot(1024) == null

        when:
        multiShard.shards << new Shard(nodes: [new Node(master: true, host: 'localhost', port: 7381)])
        multiShard.shards << new Shard(nodes: [new Node(master: true, host: 'localhost', port: 7382)])
        multiShard.shards[1].multiSlotRange.addSingle(2048, 3071)
        multiShard.shards[2].multiSlotRange.addSingle(0, 1023)
        then:
        multiShard.firstToClientSlot() == 0
        multiShard.lastToClientSlot() == 3071
        multiShard.nextToClientSlot(1024) == 2048

        when:
        multiShard.reset(false)
        multiShard.reset(true)
        then:
        multiShard.clusterMyEpoch == 0
        multiShard.clusterCurrentEpoch == 0

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
