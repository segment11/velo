package io.velo.repl.cluster

import io.velo.repl.support.JedisPoolHolder
import io.velo.test.tools.RedisServer
import spock.lang.Specification

class NodeTest extends Specification {
    def 'test base'() {
        given:
        def node = new Node()
        node.master = true
        node.slaveIndex = 0
        node.host = 'localhost'
        node.port = 6379
        node.mySelf = true
        node.followNodeId = 'xxx'
        println node

        expect:
        node.master
        node.slaveIndex == 0
        node.host == 'localhost'
        node.port == 6379
        node.mySelf
        node.followNodeId == 'xxx'
        node.nodeId().startsWith('velo_node_localhost_6379_')

        when:
        node.host = '127.0.0.1'
        node.port = 6380
        then:
        node.nodeId().startsWith('velo_node_127_0_0_1_6380_')
        node.nodeInfoPrefix().contains('myself')
        node.nodeInfoPrefix().contains('master')

        when:
        node.master = false
        node.mySelf = false
        then:
        !node.nodeInfoPrefix().contains('myself')
        node.nodeInfoPrefix().contains('slave')

        when:
        node.nodeIdFix = 'yyyy0000'
        then:
        node.nodeId() == node.nodeIdFix
    }

    def 'test exe'() {
        given:
        if (!RedisServer.isBinExists()) {
            println 'skip test exe, because redis server binary not exists'
            return
        }

        def redisServer = new RedisServer('test-node-exe').noSave().randomPort()
        Thread.start {
            redisServer.run()
        }
        def jedis = redisServer.jedis()

        and:
        def node = new Node()
        node.host = '127.0.0.1'
        node.port = redisServer.port

        when:
        def r = node.exe { j ->
            j.set('a', 'b')
        }
        then:
        r == 'OK'

        cleanup:
        JedisPoolHolder.instance.cleanUp()
        jedis.close()
        redisServer.stop()
    }
}
