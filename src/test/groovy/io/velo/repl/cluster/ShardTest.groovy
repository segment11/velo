package io.velo.repl.cluster

import spock.lang.Specification

class ShardTest extends Specification {
    def 'test all'() {
        given:
        def shard = new Shard()

        expect:
        shard.master() == null
        shard.slave(0) == null

        when:
        shard.nodes = []
        shard.nodes << new Node(master: true, host: 'localhost', port: 7379, mySelf: true)
        shard.multiSlotRange = new MultiSlotRange(list: [])
        shard.multiSlotRange.addSingle(0, 16383)
        then:
        shard.master() != null
        shard.slave(0) == null
        shard.contains(0)

        when:
        shard.nodes << new Node(master: false, slaveIndex: 0, host: 'localhost', port: 7379, followNodeId: 'xxx')
        def clusterNodesSlotRangeList = shard.clusterNodesSlotRangeList()
        clusterNodesSlotRangeList.each {
            println it
        }
        then:
        shard.slave(0) != null
        shard.slave(1) == null
        clusterNodesSlotRangeList.size() == 2

        when:
        shard.nodes.clear()
        shard.nodes << new Node(master: false, slaveIndex: 0, host: 'localhost', port: 7379, followNodeId: 'xxx')
        then:
        shard.master() == null

        when:
        shard.multiSlotRange.list.clear()
        clusterNodesSlotRangeList = shard.clusterNodesSlotRangeList()
        clusterNodesSlotRangeList.each {
            println it
        }
        then:
        1 == 1
    }
}
