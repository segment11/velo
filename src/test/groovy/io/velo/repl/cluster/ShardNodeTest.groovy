package io.velo.repl.cluster

import spock.lang.Specification

class ShardNodeTest extends Specification {
    def 'test base'() {
        given:
        def shardNode = new ShardNode()
        shardNode.master = true
        shardNode.slaveIndex = 0
        shardNode.host = 'localhost'
        shardNode.port = 6379
        shardNode.nodeId = 'xxx'

        expect:
        shardNode.master
        shardNode.slaveIndex == 0
        shardNode.host == 'localhost'
        shardNode.port == 6379
        shardNode.nodeId == 'xxx'
    }
}
