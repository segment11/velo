package io.velo.repl.cluster.watch

import io.velo.ConfForGlobal
import io.velo.persist.Consts
import io.velo.repl.LeaderSelector
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.repl.support.JedisPoolHolder
import spock.lang.Specification

class FailoverManagerTest extends Specification {
    def 'test base'() {
        given:
        def fm = FailoverManager.instance
        fm.zookeeperVeloMetaBasePath = '/velo/failover_manager'

        expect:
        fm.oneEndpointStatusMapByClusterName.size() == 0
        fm.skipOneClusterNameSet.size() == 0

        when:
        fm.addOneEndpointStatusMapByClusterNamePostBySlave('localhost:27380', [:])
        then:
        1 == 1

        when:
        fm.addSkipOneClusterName('cluster1')
        then:
        fm.skipOneClusterNameSet.size() == 1

        when:
        fm.removeSkipOneClusterName('cluster1')
        then:
        fm.skipOneClusterNameSet.size() == 0
    }

    def 'test update one cluster meta and check failover'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:27379'

        def fm = FailoverManager.instance
        fm.zookeeperVeloMetaBasePath = '/velo/failover_manager'
        ConfForGlobal.zookeeperRootPath = fm.zookeeperVeloMetaBasePath

        def leaderSelector = LeaderSelector.instance

        boolean doThisCase = Consts.checkConnectAvailable()
        if (!doThisCase) {
            ConfForGlobal.zookeeperConnectString = null
            println 'zookeeper not running, skip'
        } else {
            ConfForGlobal.zookeeperConnectString = 'localhost:2181'
        }

        when:
        if (doThisCase) {
            leaderSelector.tryConnectAndGetMasterListenAddress(true)
            fm.addOneMetaToZookeeper('cluster1', [new HostAndPort('localhost', 7379)])
            fm.addOneMetaToZookeeper('cluster1', [new HostAndPort('localhost', 7379)])
        }
        then:
        true

        when:
        if (doThisCase) {
            fm.removeOneMetaFromZookeeper('cluster1')
            fm.removeOneMetaFromZookeeper('cluster1')
        }
        then:
        true

        when:
        List<Shard> shards = []
        def shard0 = new Shard()
        def shard1 = new Shard()
        shards << shard0
        shards << shard1
        shard0.nodes << new Node(master: true, host: 'localhost', port: 7379, mySelf: true, nodeIdFix: 'aaa')
        shard0.nodes << new Node(master: false, slaveIndex: 0, host: 'localhost', port: 7380, nodeIdFix: 'xxx', followNodeId: 'aaa')
        shard1.nodes << new Node(master: true, host: 'localhost', port: 8379, nodeIdFix: 'bbb')
        shard1.nodes << new Node(master: false, slaveIndex: 0, host: 'localhost', port: 8380, nodeIdFix: 'yyy', followNodeId: 'bbb')
        if (doThisCase) {
            fm.updateZookeeperOneMetaAfterDoFailover('cluster1', shards)
        }
        then:
        true

        when:
        if (doThisCase) {
            fm.loopCount = 9
            fm.addOneEndpointStatusMapByClusterNamePostBySlave('localhost:27380', [:])
            fm.checkFailover()
        }
        then:
        true

        when:
        if (doThisCase) {
            leaderSelector.hasLeadershipLocalMocked = false
            fm.checkFailover()
        }
        then:
        true

        when:
        if (doThisCase) {
            fm.clearOneEndpointStatusMapByClusterName()
            fm.clearOneEndpointStatusMapByClusterNamePostBySlave()
            fm.checkFailover()
        }
        then:
        true

        when:
        def clusterxNodesArgs = '''
aaa localhost 7379 slave xxx
bbb localhost 7380 master - 8192-16383
xxx localhost 17379 master - 0-8191
yyy localhost 17380 slave bbb
'''.trim()
        if (doThisCase) {
            fm.addDelayRestartCheckForFailedHostAndPortNode(new HostAndPort('localhost', 7379), '')
            fm.addDelayRestartCheckForFailedHostAndPortNode(new HostAndPort('localhost', 7379), clusterxNodesArgs)
        }
        then:
        true

        when:
        if (doThisCase) {
            fm.mockSetNodes = true
            fm.checkIfServerRestartAndThenSetNodes(FailoverManager.DELAY_RESTART_CHECK_FOR_FAILED_HOST_AND_PORT_NODE_NAME_PREFIX + 'localhost:7379')
        }
        then:
        true

        cleanup:
        fm.mockSetNodes = false
        leaderSelector.cleanUp()
    }

    def 'test do failover'() {
        given:
        def fm = FailoverManager.instance
        def failHostAndPort = new HostAndPort('localhost', 7379)

        boolean doThisCase = Consts.checkConnectAvailable('localhost', 7380)
        if (!doThisCase) {
            println 'redis server not running, skip'
        }

        when:
        fm.addOneMeta('cluster1', [
                new HostAndPort('localhost', 7379)
        ])
        fm.doFailover('cluster1', failHostAndPort)
        then:
        true

        when:
        boolean r
        // prepare velo cluster, 2 shards, first shard, master node 7379, slave node 17379, second shard, master node 7380, slave node 17380
        if (doThisCase) {
            fm.addOneMeta('cluster1', [
                    new HostAndPort('localhost', 7379),
                    new HostAndPort('localhost', 7380)
            ])
            fm.doFailover('cluster1', failHostAndPort)

            def jedisPool = JedisPoolHolder.instance.createIfNotCached('localhost', 7380)
            r = JedisPoolHolder.exe(jedisPool) { jedis ->
                def clusterNodes = jedis.clusterNodes()
                def lines = clusterNodes.readLines().collect { it.trim() }.findAll { it }
                lines.find {
                    it.contains('slave') && it.contains('7379')
                } != null
            }
        } else {
            r = true
        }
        then:
        r

        /*
${nodeId} ${ip} ${port} master -
${nodeId} ${ip} ${port} master - 0
${nodeId} ${ip} ${port} master - 0-8191 10000
${nodeId} ${ip} ${port} slave ${primaryNodeId}
         */
        when:
        fm.mockSetNodes = true
        def clusterxNodesArgs = '''
aaa localhost 7379 master - 0-8191
bbb localhost 7380 master - 8192-16383
xxx localhost 17379 slave aaa
yyy localhost 17380 slave bbb
'''.trim()
        def lines = clusterxNodesArgs.readLines().collect { it.trim() }.findAll { it }
        boolean isZkListen = Consts.checkConnectAvailable()
        if (isZkListen) {
            ConfForGlobal.netListenAddresses = 'localhost:27379'

            fm.zookeeperVeloMetaBasePath = '/velo/failover_manager'
            ConfForGlobal.zookeeperRootPath = fm.zookeeperVeloMetaBasePath

            def leaderSelector = LeaderSelector.instance
            ConfForGlobal.zookeeperConnectString = 'localhost:2181'
            leaderSelector.tryConnectAndGetMasterListenAddress(true)

            fm.doFailoverOneShard('cluster1', failHostAndPort, lines)
        }
        then:
        true

        when:
        failHostAndPort.host = '127.0.0.1'
        // shard not find
        if (isZkListen) {
            fm.doFailoverOneShard('cluster1', failHostAndPort, lines)
        }
        then:
        true

        when:
        failHostAndPort.host = 'localhost'
        failHostAndPort.port = 17379
        // slave node need not failover
        if (isZkListen) {
            fm.doFailoverOneShard('cluster1', failHostAndPort, lines)
        }
        then:
        true

        when:
        failHostAndPort.port = 7379
        clusterxNodesArgs = '''
aaa localhost 7379 master - 0-8191
bbb localhost 7380 master - 8192-16383
yyy localhost 17380 slave bbb
'''.trim()
        lines = clusterxNodesArgs.readLines().collect { it.trim() }.findAll { it }
        // no slave node can be promoted
        if (isZkListen) {
            fm.doFailoverOneShard('cluster1', failHostAndPort, lines)
        }
        then:
        true
    }
}
