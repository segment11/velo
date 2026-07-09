package io.velo.repl.cluster.watch

import io.velo.ConfForGlobal
import io.velo.HostAndPort
import io.velo.persist.Consts
import io.velo.repl.LeaderSelector
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.repl.support.JedisPoolHolder
import spock.lang.Specification

class FailoverManagerTest extends Specification {
    def setupSpec() {
        Consts.downloadAndStartZookeeper()
    }

    def cleanupSpec() {
        Consts.stopZookeeper()
    }

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
        ConfForGlobal.netListenAddress = 'localhost:27379'

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
            ConfForGlobal.netListenAddress = 'localhost:27379'

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
        failHostAndPort = new HostAndPort('127.0.0.1', failHostAndPort.port)
        // shard not find
        if (isZkListen) {
            fm.doFailoverOneShard('cluster1', failHostAndPort, lines)
        }
        then:
        true

        when:
        failHostAndPort = new HostAndPort('localhost', 17379)
        // slave node need not failover
        if (isZkListen) {
            fm.doFailoverOneShard('cluster1', failHostAndPort, lines)
        }
        then:
        true

        when:
        failHostAndPort = new HostAndPort('localhost', 7379)
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

    def 'do failover one shard picks slave with highest replication offset'() {
        given:
        def fm = FailoverManager.instance
        fm.mockSetNodes = true
        fm.clearOneEndpointStatusMapByClusterName()
        def failHostAndPort = new HostAndPort('localhost', 7379)
        def lines = '''
aaa localhost 7379 master - 0-8191
xxx localhost 17379 slave aaa
zzz localhost 17381 slave aaa
'''.trim().readLines().collect { it.trim() }.findAll { it }

        fm.delayRestartCheckHandlerForTest = { HostAndPort ignored, String clusterxNodesArgs -> null }
        fm.slaveReplOffsetProviderForTest = { Node node ->
            if (node.port == 17379) {
                return 100L
            }
            if (node.port == 17381) {
                return 200L
            }
            throw new IllegalStateException("unexpected node port=${node.port}")
        }

        when:
        fm.doFailoverOneShard('cluster1', failHostAndPort, lines)

        then:
        fm.oneEndpointStatusMapByClusterName['cluster1'].keySet() == [new HostAndPort('localhost', 17381)] as Set

        cleanup:
        fm.mockSetNodes = false
        fm.slaveReplOffsetProviderForTest = null
        fm.delayRestartCheckHandlerForTest = null
        fm.clearOneEndpointStatusMapByClusterName()
    }

    def 'do failover one shard skips slaves whose replication offset cannot be read'() {
        given:
        def fm = FailoverManager.instance
        fm.mockSetNodes = true
        fm.clearOneEndpointStatusMapByClusterName()
        def failHostAndPort = new HostAndPort('localhost', 7379)
        def lines = '''
aaa localhost 7379 master - 0-8191
xxx localhost 17379 slave aaa
zzz localhost 17381 slave aaa
'''.trim().readLines().collect { it.trim() }.findAll { it }

        fm.delayRestartCheckHandlerForTest = { HostAndPort ignored, String clusterxNodesArgs -> null }
        fm.slaveReplOffsetProviderForTest = { Node node ->
            if (node.port == 17379) {
                throw new IllegalStateException('offset unavailable')
            }
            if (node.port == 17381) {
                return 200L
            }
            throw new IllegalStateException('unexpected node port=' + node.port)
        }

        when:
        fm.doFailoverOneShard('cluster1', failHostAndPort, lines)

        then:
        noExceptionThrown()
        fm.oneEndpointStatusMapByClusterName['cluster1'].keySet() == [new HostAndPort('localhost', 17381)] as Set

        cleanup:
        fm.mockSetNodes = false
        fm.slaveReplOffsetProviderForTest = null
        fm.delayRestartCheckHandlerForTest = null
        fm.clearOneEndpointStatusMapByClusterName()
    }

    def 'do failover retries setnodes and registers un-acked nodes for rejoin re-push (phase 2)'() {
        given:
        def fm = FailoverManager.instance
        fm.mockSetNodes = true
        fm.clearOneEndpointStatusMapByClusterName()
        def failHostAndPort = new HostAndPort('localhost', 7379)
        def lines = '''
aaa localhost 7379 master - 0-8191
bbb localhost 7380 master - 8192-16383
xxx localhost 17379 slave aaa
yyy localhost 17380 slave bbb
'''.trim().readLines().collect { it.trim() }.findAll { it }

        and: 'capture rejoin re-push registrations'
        def registered = []
        fm.delayRestartCheckHandlerForTest = { HostAndPort hp, String clusterxNodesArgs -> registered << hp; null }

        and: 'promote 17379'
        fm.slaveReplOffsetProviderForTest = { Node node -> node.port == 17379 ? 200L : 100L }

        and: 'node 7380 is unreachable (throws); node 17380 is up but rejects (non-OK); count per-node attempts'
        def attemptsByNode = [:].withDefault { 0 }
        fm.setNodesResultProviderForTest = { HostAndPort hp ->
            attemptsByNode[hp.port] = (attemptsByNode[hp.port] as int) + 1
            if (hp.port == 7380) {
                throw new RuntimeException('simulated unreachable')
            }
            if (hp.port == 17380) {
                return 'ERR stale cluster version'
            }
            'OK'
        }

        when:
        fm.doFailoverOneShard('cluster1', failHostAndPort, lines)

        then: 'an unreachable or rejecting node does not abort the broadcast'
        noExceptionThrown()

        and: 'both the throwing and the non-OK node were retried SET_NODES_MAX_ATTEMPTS times'
        attemptsByNode[7380] == FailoverManager.SET_NODES_MAX_ATTEMPTS
        attemptsByNode[17380] == FailoverManager.SET_NODES_MAX_ATTEMPTS

        and: 'a reachable, accepting node got exactly one successful setnodes'
        attemptsByNode[17379] == 1

        and: 'the failed node and both un-acked nodes are registered for rejoin re-push'
        registered.find { it.port == 7379 } != null
        registered.find { it.port == 7380 } != null
        registered.find { it.port == 17380 } != null

        cleanup:
        fm.mockSetNodes = false
        fm.setNodesResultProviderForTest = null
        fm.slaveReplOffsetProviderForTest = null
        fm.delayRestartCheckHandlerForTest = null
        fm.clearOneEndpointStatusMapByClusterName()
    }

    def 'do failover if need skips failover when slave reports failed host ping ok'() {
        given:
        def fm = FailoverManager.instance
        fm.clearOneEndpointStatusMapByClusterName()
        fm.clearOneEndpointStatusMapByClusterNamePostBySlave()

        def failedHostAndPort = new HostAndPort('localhost', 7379)
        def peerHostAndPort = new HostAndPort('localhost', 7380)
        def failedStatus = new OneEndpointStatus()
        10.times {
            failedStatus.addStatus(OneEndpointStatus.Status.PING_FAIL)
        }
        def failedStatusBySlave = new OneEndpointStatus()
        failedStatusBySlave.addStatus(OneEndpointStatus.Status.PING_OK)
        def peerStatus = new OneEndpointStatus()
        peerStatus.addStatus(OneEndpointStatus.Status.PING_OK)
        fm.addOneMeta('cluster1', [failedHostAndPort, peerHostAndPort])
        fm.setOneEndpointStatus('cluster1', failedHostAndPort, failedStatus)
        fm.setOneEndpointStatus('cluster1', peerHostAndPort, peerStatus)
        fm.addOneEndpointStatusMapByClusterNamePostBySlave('localhost:17379', [
                cluster1: [
                        (failedHostAndPort): failedStatusBySlave,
                ]
        ])

        when:
        fm.doFailoverIfNeed()

        then:
        noExceptionThrown()
        fm.oneEndpointStatusMapByClusterName['cluster1'].keySet() == [failedHostAndPort, peerHostAndPort] as Set

        cleanup:
        fm.clearOneEndpointStatusMapByClusterName()
        fm.clearOneEndpointStatusMapByClusterNamePostBySlave()
    }

    def 'stale slave posted endpoint status is not trusted for failover suppression'() {
        given:
        def fm = FailoverManager.instance
        fm.clearOneEndpointStatusMapByClusterNamePostBySlave()
        fm.slavePostedStatusMaxAgeMillisForTest = 1L

        def failedHostAndPort = new HostAndPort('localhost', 7379)
        def statusBySlave = new OneEndpointStatus()
        statusBySlave.addStatus(OneEndpointStatus.Status.PING_OK)
        fm.addOneEndpointStatusMapByClusterNamePostBySlave('localhost:17379', [
                cluster1: [
                        (failedHostAndPort): statusBySlave,
                ]
        ])

        when:
        Thread.sleep(5L)

        then:
        !fm.isPingOkPostBySlaveFresh('cluster1', failedHostAndPort)

        cleanup:
        fm.slavePostedStatusMaxAgeMillisForTest = null
        fm.clearOneEndpointStatusMapByClusterNamePostBySlave()
    }

    def 'test compute next cluster version'() {
        given:
        def fm = FailoverManager.instance
        def a = new HostAndPort('localhost', 7379)
        def b = new HostAndPort('localhost', 7380)
        def c = new HostAndPort('localhost', 7381)

        when: 'all epochs readable -> max + 1'
        fm.nodeClusterEpochProviderForTest = { HostAndPort hp ->
            hp.port == 7379 ? 5 : (hp.port == 7380 ? 8 : 3)
        }
        def v = fm.computeNextClusterVersion([a, b, c])
        then:
        v == 9

        when: 'an unreachable node is skipped, version derived from the rest'
        fm.nodeClusterEpochProviderForTest = { HostAndPort hp ->
            if (hp.port == 7380) throw new RuntimeException('node down')
            hp.port == 7379 ? 5 : 3
        }
        v = fm.computeNextClusterVersion([a, b, c])
        then:
        v == 6

        when: 'no node reports an epoch -> version starts at 1'
        fm.nodeClusterEpochProviderForTest = { HostAndPort hp -> throw new RuntimeException('node down') }
        v = fm.computeNextClusterVersion([a, b, c])
        then:
        v == 1

        cleanup:
        fm.nodeClusterEpochProviderForTest = null
    }
}
