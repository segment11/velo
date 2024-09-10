package io.velo.repl.cluster

import spock.lang.Specification

class ShardTest extends Specification {
    def 'test all'() {
        given:
        def shard = new Shard()
        println shard

        expect:
        shard.master() == null
        shard.slave(0) == null
        shard.mySelfNode() == null

        when:
        shard.nodes = []
        shard.nodes << new Node(master: true, host: 'localhost', port: 7379, mySelf: true)
        shard.multiSlotRange = new MultiSlotRange(list: [])
        shard.multiSlotRange.addSingle(0, 16383)
        println shard
        then:
        shard.master() != null
        shard.slave(0) == null
        shard.mySelfNode() != null
        shard.contains(0)

        when:
        shard.nodes << new Node(master: false, slaveIndex: 0, host: 'localhost', port: 7379, followNodeId: 'xxx')
        println shard
        def clusterNodesSlotRangeList = shard.clusterNodesSlotRangeList()
        clusterNodesSlotRangeList.each {
            println it
        }
        then:
        shard.slave(0) != null
        shard.slave(1) == null
        shard.mySelfNode() != null
        clusterNodesSlotRangeList.size() == 2

        when:
        shard.nodes[0].mySelf = false
        then:
        shard.mySelfNode() == null

        when:
        shard.nodes.clear()
        shard.nodes << new Node(master: false, slaveIndex: 0, host: 'localhost', port: 7379, followNodeId: 'xxx')
        println shard
        then:
        shard.master() == null

        when:
        shard.multiSlotRange.list.clear()
        println shard
        clusterNodesSlotRangeList = shard.clusterNodesSlotRangeList()
        clusterNodesSlotRangeList.each {
            println it
        }
        then:
        1 == 1

        when:
        shard.migratingToHost = 'localhost'
        shard.migratingToPort = 7379
        shard.importMigratingSlot = 0
        shard.exportMigratingSlot = 1
        then:
        shard.migratingToHost == 'localhost'
        shard.migratingToPort == 7379
        shard.importMigratingSlot == 0
        shard.exportMigratingSlot == 1
    }
}
