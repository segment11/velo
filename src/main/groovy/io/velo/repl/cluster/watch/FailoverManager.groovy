package io.velo.repl.cluster.watch

import com.fasterxml.jackson.databind.ObjectMapper
import com.github.kevinsawicki.http.HttpRequest
import groovy.transform.CompileStatic
import groovy.util.logging.Slf4j
import io.velo.ConfForGlobal
import io.velo.repl.LeaderSelector
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.repl.support.ExtendProtocolCommand
import io.velo.repl.support.JedisPoolHolder
import org.jetbrains.annotations.TestOnly
import org.jetbrains.annotations.VisibleForTesting

import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.ConcurrentSkipListSet

/**
 * The FailoverManager class is responsible for managing failover operations in a cluster environment.
 * It monitors the status of cluster nodes, detects failures, and performs failover if necessary.
 */
@CompileStatic
@Slf4j
@Singleton
class FailoverManager {
    /**
     * The base path in Zookeeper where cluster metadata is stored.
     */
    String zookeeperVeloMetaBasePath

    /**
     * A map that stores the status of cluster endpoints, grouped by cluster name.
     */
    private final Map<String, Map<HostAndPort, OneEndpointStatus>> oneEndpointStatusMapByClusterName = [:]

    /**
     * Returns a copy of the map that stores the status of cluster endpoints, grouped by cluster name.
     * @return A copy of the map of cluster endpoint statuses.
     */
    synchronized Map<String, Map<HostAndPort, OneEndpointStatus>> getOneEndpointStatusMapByClusterName() {
        return new HashMap<String, Map<HostAndPort, OneEndpointStatus>>(oneEndpointStatusMapByClusterName)
    }

    /**
     * Clears the map that stores the status of cluster endpoints, grouped by cluster name.
     * Intended for testing purposes only.
     */
    @TestOnly
    void clearOneEndpointStatusMapByClusterName() {
        oneEndpointStatusMapByClusterName.clear()
    }

    /**
     * A concurrent map that stores the status of cluster endpoints posted by slaves, grouped by slave listen address.
     */
    private final ConcurrentHashMap<String, Map<String, Map<HostAndPort, OneEndpointStatus>>> oneEndpointStatusMapByClusterNamePostBySlave = new ConcurrentHashMap<>()

    /**
     * Adds the status of cluster endpoints posted by a slave to the map.
     * @param slaveListenAddress The listen address of the slave.
     * @param oneEndpointStatusMapByClusterName The map of cluster endpoint statuses posted by the slave.
     */
    void addOneEndpointStatusMapByClusterNamePostBySlave(String slaveListenAddress, Map<String, Map<HostAndPort, OneEndpointStatus>> oneEndpointStatusMapByClusterName) {
        oneEndpointStatusMapByClusterNamePostBySlave[slaveListenAddress] = oneEndpointStatusMapByClusterName
    }

    /**
     * Clears the map that stores the status of cluster endpoints posted by slaves.
     * Intended for testing purposes only.
     */
    @TestOnly
    void clearOneEndpointStatusMapByClusterNamePostBySlave() {
        oneEndpointStatusMapByClusterNamePostBySlave.clear()
    }

    /**
     * A concurrent skip list set that stores cluster names to be skipped by the failover manager.
     */
    private ConcurrentSkipListSet<String> skipOneClusterNameSet = []

    /**
     * Returns the set of cluster names to be skipped by the failover manager.
     * @return The set of cluster names to be skipped.
     */
    ConcurrentSkipListSet<String> getSkipOneClusterNameSet() {
        return skipOneClusterNameSet
    }

    /**
     * Adds a cluster name to the set of cluster names to be skipped.
     * @param oneClusterName The name of the cluster to be skipped.
     */
    void addSkipOneClusterName(String oneClusterName) {
        skipOneClusterNameSet.add(oneClusterName)
    }

    /**
     * Removes a cluster name from the set of cluster names to be skipped.
     * @param oneClusterName The name of the cluster to be removed from the skip list.
     */
    void removeSkipOneClusterName(String oneClusterName) {
        skipOneClusterNameSet.remove(oneClusterName)
    }

    /**
     * Adds a new cluster metadata to the map of cluster endpoint statuses.
     * Intended for testing purposes only.
     * @param oneClusterName The name of the cluster.
     * @param masterHostAndPortList The list of host and port pairs for the master nodes.
     */
    @TestOnly
    synchronized void addOneMeta(String oneClusterName, List<HostAndPort> masterHostAndPortList) {
        Map<HostAndPort, OneEndpointStatus> newOneEndpointStatusMap = [:]
        masterHostAndPortList.each { hostAndPort ->
            newOneEndpointStatusMap[hostAndPort] = new OneEndpointStatus()
        }
        oneEndpointStatusMapByClusterName[oneClusterName] = newOneEndpointStatusMap
    }

    private final LeaderSelector leaderSelector = LeaderSelector.instance

    /**
     * Adds a new cluster metadata to the map of cluster endpoint statuses and updates the metadata in Zookeeper.
     * @param oneClusterName The name of the cluster.
     * @param masterHostAndPortList The list of host and port pairs for the master nodes.
     */
    synchronized void addOneMetaToZookeeper(String oneClusterName, List<HostAndPort> masterHostAndPortList) {
        Map<HostAndPort, OneEndpointStatus> newOneEndpointStatusMap = [:]
        masterHostAndPortList.each { hostAndPort ->
            newOneEndpointStatusMap[hostAndPort] = new OneEndpointStatus()
        }
        oneEndpointStatusMapByClusterName[oneClusterName] = newOneEndpointStatusMap

        def objectMapper = new ObjectMapper()
        def oneClusterMeta = new OneClusterMeta()
        oneClusterMeta.masterHostAndPortList = masterHostAndPortList
        def data = objectMapper.writeValueAsString(oneClusterMeta)

        def client = leaderSelector.client
        // zookeeperVeloMetaBasePath already exists
        def nodeStat = client.checkExists().forPath(zookeeperVeloMetaBasePath + '/' + oneClusterName)
        if (nodeStat == null) {
            client.create().forPath(zookeeperVeloMetaBasePath + '/' + oneClusterName, data.bytes)
        } else {
            client.setData().forPath(zookeeperVeloMetaBasePath + '/' + oneClusterName, data.bytes)
        }
        log.warn 'failover manager update zookeeper one cluster meta success, cluster name={}', oneClusterName
    }

    /**
     * Removes a cluster metadata from the map of cluster endpoint statuses and updates Zookeeper.
     * @param oneClusterName The name of the cluster.
     */
    synchronized void removeOneMetaFromZookeeper(String oneClusterName) {
        oneEndpointStatusMapByClusterName.remove(oneClusterName)

        def client = leaderSelector.client
        def nodeStat = client.checkExists().forPath(zookeeperVeloMetaBasePath + '/' + oneClusterName)
        if (nodeStat != null) {
            client.delete().forPath(zookeeperVeloMetaBasePath + '/' + oneClusterName)
        }
        log.warn 'failover manager remove zookeeper one cluster meta success, cluster name={}', oneClusterName
    }

    /**
     * Updates the cluster metadata in Zookeeper after performing a failover.
     * @param oneClusterName The name of the cluster.
     * @param shards The list of shards in the cluster.
     */
    @VisibleForTesting
    void updateZookeeperOneMetaAfterDoFailover(String oneClusterName, List<Shard> shards) {
        log.warn 'failover manager update zookeeper one cluster meta after do failover, cluster name={}', oneClusterName
        def masterHostAndPortList = shards.collect { ss ->
            ss.nodes.find { nn -> nn.master }
        }.collect { nn -> new HostAndPort(nn.host, nn.port) }

        if (mockSetNodes) {
            addOneMeta(oneClusterName, masterHostAndPortList)
        } else {
            addOneMetaToZookeeper(oneClusterName, masterHostAndPortList)
        }
    }

    /**
     * The prefix for the Zookeeper node name that indicates a delayed restart check for a failed host and port.
     */
    @VisibleForTesting
    static final String DELAY_RESTART_CHECK_FOR_FAILED_HOST_AND_PORT_NODE_NAME_PREFIX = 'delay_restart_'

    /**
     * Adds a Zookeeper node to indicate a delayed restart check for a failed host and port.
     * @param failHostAndPort The host and port of the failed node.
     * @param clusterxNodesArgs The clusterx nodes argument string.
     */
    @VisibleForTesting
    void addDelayRestartCheckForFailedHostAndPortNode(HostAndPort failHostAndPort, String clusterxNodesArgs) {
        log.warn 'failover manager add delay restart check for failed host and port node, host and port={}:{}', failHostAndPort.host, failHostAndPort.port
        def path = zookeeperVeloMetaBasePath + '/' + DELAY_RESTART_CHECK_FOR_FAILED_HOST_AND_PORT_NODE_NAME_PREFIX + failHostAndPort.toString()

        def client = leaderSelector.client
        def nodeStat = client.checkExists().forPath(path)
        if (nodeStat == null) {
            client.create().forPath(path, clusterxNodesArgs.bytes)
        } else {
            client.setData().forPath(path, clusterxNodesArgs.bytes)
        }
    }

    private final JedisPoolHolder jedisPoolHolder = JedisPoolHolder.instance

    /**
     * Checks if a server has restarted and performs the necessary set nodes operation.
     * @param nodeName The name of the Zookeeper node.
     */
    @VisibleForTesting
    void checkIfServerRestartAndThenSetNodes(String nodeName) {
        def array = nodeName.substring(DELAY_RESTART_CHECK_FOR_FAILED_HOST_AND_PORT_NODE_NAME_PREFIX.length()).split(':')
        def hostAndPort = new HostAndPort(array[0], array[1] as int)
        try {
            def jedisPool = jedisPoolHolder.create(hostAndPort.host, hostAndPort.port)
            JedisPoolHolder.exe(jedisPool) { jedis ->
                jedis.ping()
            }
        } catch (Exception e) {
            log.error 'failover manager ping fail when check if server restart, error={}, host and port={}:{}',
                    e.message, hostAndPort.host, hostAndPort.port
            return
        }

        def client = leaderSelector.client
        try {
            def clusterxNodesArgs = new String(client.getData().forPath(zookeeperVeloMetaBasePath + '/' + nodeName))
            def jedisPool = jedisPoolHolder.create(hostAndPort.host, hostAndPort.port)
            String result
            if (mockSetNodes) {
                result = 'OK'
            } else {
                result = JedisPoolHolder.exe(jedisPool) { jedis ->
                    def command = new ExtendProtocolCommand('clusterx')
                    byte[] r = jedis.sendCommand(command, 'setnodes'.bytes, clusterxNodesArgs.bytes) as byte[]
                    new String(r)
                }
            }

            if ('OK' != result) {
                log.error 'failover manager clusterx setnodes fail when check server restart ok, result={}, target host and port={}:{}',
                        result, hostAndPort.host, hostAndPort.port
            } else {
                log.warn 'failover manager clusterx setnodes ok when check server restart ok, result={}, target host and port={}:{}',
                        result, hostAndPort.host, hostAndPort.port
                // remove this node
                client.delete().forPath(zookeeperVeloMetaBasePath + '/' + nodeName)
                log.warn 'failover manager remove delay restart check for failed host and port node success, host and port={}:{}', hostAndPort.host, hostAndPort.port
            }
        } catch (Exception e) {
            log.error 'failover manager set nodes fail when check server restart ok, error={}, host and port={}:{}',
                    e.message, hostAndPort.host, hostAndPort.port
        }
    }

    /**
     * Checks the status of the cluster and performs failover if necessary.
     */
    void checkFailover() {
        def client = leaderSelector.client
        if (client == null) {
            // not init yet
            return
        }

        var children = client.getChildren().forPath(zookeeperVeloMetaBasePath)

        for (nodeName in children) {
            if (nodeName == ConfForGlobal.LEADER_LATCH_NODE_NAME) {
                continue
            }

            if (nodeName.startsWith(DELAY_RESTART_CHECK_FOR_FAILED_HOST_AND_PORT_NODE_NAME_PREFIX)) {
                checkIfServerRestartAndThenSetNodes(nodeName)
                continue
            }

            def oneClusterName = nodeName
            if (skipOneClusterNameSet.contains(oneClusterName)) {
                log.debug 'fail manager skip target cluster, name={}', oneClusterName
                continue
            }

            Map<HostAndPort, OneEndpointStatus> oneEndpointStatusMap = oneEndpointStatusMapByClusterName[oneClusterName]
            if (oneEndpointStatusMap == null) {
                oneEndpointStatusMap = [:]
                oneEndpointStatusMapByClusterName[oneClusterName] = oneEndpointStatusMap
            }

            def data = client.getData().forPath(zookeeperVeloMetaBasePath + '/' + oneClusterName)
            def objectMapper = new ObjectMapper()
            def oneClusterMeta = objectMapper.readValue(data, OneClusterMeta.class)
            if (oneClusterMeta.masterHostAndPortList) {
                for (hostAndPort in oneClusterMeta.masterHostAndPortList) {
                    def oneEndpointStatus = oneEndpointStatusMap[hostAndPort]
                    if (oneEndpointStatus == null) {
                        oneEndpointStatus = new OneEndpointStatus()
                        oneEndpointStatusMap[hostAndPort] = oneEndpointStatus
                    }

                    try {
                        def jedisPool = jedisPoolHolder.create(hostAndPort.host, hostAndPort.port)
                        JedisPoolHolder.exe(jedisPool) { jedis ->
                            jedis.ping()
                        }
                        oneEndpointStatus.addStatus(OneEndpointStatus.Status.PING_OK)
                    } catch (Exception e) {
                        log.error 'failover manager ping fail, error={}, cluster name={}', e.message, oneClusterName
                        oneEndpointStatus.addStatus(OneEndpointStatus.Status.PING_FAIL)
                    }
                }
            }
        }

        postStatusToLeader()
    }

    /**
     * The loop count for the checkFailover method.
     */
    @VisibleForTesting
    int loopCount = 0

    /**
     * Posts the status of the cluster endpoints to the leader.
     */
    void postStatusToLeader() {
        loopCount++

        def isSelfLeader = leaderSelector.hasLeadership()
        if (isSelfLeader) {
            if (loopCount % 10 == 0) {
                log.info 'failover manager as leader, i am alive, loop count={}', loopCount
            }
            doFailoverIfNeed()
            return
        }

        def leaderListenAddress = leaderSelector.getLastGetMasterListenAddressAsSlave()
        if (!leaderListenAddress) {
            log.warn 'failover manager as slave, leader is null'
            return
        }

        def tmp = new TmpEndpointsStatus(ConfForGlobal.netListenAddresses, oneEndpointStatusMapByClusterName)
        def req = HttpRequest.post('http://' + leaderListenAddress + '/velo/failover_manager/endpoints_status/slave_post')
                .connectTimeout(1000)
                .readTimeout(1000)
                .send(new ObjectMapper().writeValueAsString(tmp))
        if (200 != req.code()) {
            log.error 'failover manager post status to leader fail, code={}, to leader listen address={}', req.code(), leaderListenAddress
            log.error req.body()
        } else {
            if (loopCount % 10 == 0) {
                log.info 'failover manager post status to leader success, loop count={}, to leader listen address={}', loopCount, leaderListenAddress
            }
        }
    }

    /**
     * Performs failover if necessary based on the endpoint status.
     */
    @VisibleForTesting
    void doFailoverIfNeed() {
        for (entry in oneEndpointStatusMapByClusterName.entrySet()) {
            def oneClusterName = entry.key
            def oneEndpointStatusMap = entry.value

            for (entry2 in oneEndpointStatusMap.entrySet()) {
                def hostAndPort = entry2.key
                def oneEndpointStatus = entry2.value

                if (oneEndpointStatus.isPingOk()) {
                    continue
                }

                boolean isPingOkPostBySlave = false
                // check is ping ok post by slave
                for (entry3 in oneEndpointStatusMapByClusterNamePostBySlave.entrySet()) {
                    def slaveListenAddress = entry3.key
                    def oneEndpointStatusMapByClusterNamePostBySlave = entry3.value

                    def oneEndpointStatusMapByClusterName = oneEndpointStatusMapByClusterNamePostBySlave[oneClusterName]
                    if (oneEndpointStatusMapByClusterName == null) {
                        continue
                    }

                    def oneEndpointStatusBySlave = oneEndpointStatusMapByClusterName[hostAndPort]
                    if (oneEndpointStatus == null) {
                        continue
                    }

                    if (oneEndpointStatusBySlave.isPingOk()) {
                        isPingOkPostBySlave = true
                        break
                    }
                }

                if (isPingOkPostBySlave) {
                    continue
                }

                doFailover(oneClusterName, hostAndPort)
            }
        }
    }

    /**
     * Performs a failover for the specified cluster and host and port.
     * @param oneClusterName The name of the cluster.
     * @param failHostAndPort The host and port of the failed node.
     */
    @VisibleForTesting
    void doFailover(String oneClusterName, HostAndPort failHostAndPort) {
        log.warn 'failover manager do failover, cluster name={}, fail host and port={}:{}', oneClusterName, failHostAndPort.host, failHostAndPort.port

        def oneEndpointStatusMap = oneEndpointStatusMapByClusterName[oneClusterName]
        for (entry in oneEndpointStatusMap.entrySet()) {
            def hostAndPort = entry.key
            def oneEndpointStatus = entry.value

            if (hostAndPort == failHostAndPort) {
                continue
            }

            if (!oneEndpointStatus.isPingOk()) {
                continue
            }

            log.warn 'failover manager do failover, cluster name={}, query cluster nodes by target host and port={}:{}',
                    oneClusterName, hostAndPort.host, hostAndPort.port
            def jedisPool = jedisPoolHolder.create(hostAndPort.host, hostAndPort.port)
            def r = JedisPoolHolder.exe(jedisPool) { jedis ->
                jedis.clusterNodes()
            }

            def lines = r.readLines().collect { it.trim() }.findAll { it }
            doFailoverOneShard(oneClusterName, failHostAndPort, lines)
            break
        }
    }

    @TestOnly
    boolean mockSetNodes

    /**
     * Performs a failover for a shard in the specified cluster.
     * Intended for testing purposes only.
     * @param oneClusterName
     * @param failHostAndPort
     * @param lines
     */
    @VisibleForTesting
    void doFailoverOneShard(String oneClusterName, HostAndPort failHostAndPort, List<String> lines) {
        ArrayList<Shard> shards = []
        lines.findAll { it.contains('master') }.each {
            def arr = it.split(' ')
            def node = new Node()
            node.nodeIdFix = arr[0]
            node.host = arr[1]
            node.port = arr[2] as int
            node.master = true

            def shard = new Shard()
            shard.nodes << node

            def multiSlotRange = shard.multiSlotRange
            if (arr.length > 4) {
                for (i in 4..<arr.length) {
                    def tmp = arr[i]
                    if (tmp[0] == '-') {
                        continue
                    }

                    if (tmp.contains('-')) {
                        def subArr = tmp.split('-')
                        multiSlotRange.addSingle(subArr[0] as int, subArr[1] as int)
                        continue
                    }

                    multiSlotRange.addSingle(tmp as int, tmp as int)
                }
            }

            shards << shard
        }

        lines.findAll { !it.contains('master') }.each {
            def arr = it.split(' ')
            def node = new Node()
            node.nodeIdFix = arr[0]
            node.host = arr[1]
            node.port = arr[2] as int
            node.master = false
            def followNodeId = arr[4]
            node.followNodeId = followNodeId

            def shard = shards.find { ss ->
                ss.nodes.find { nn ->
                    nn.nodeId() == followNodeId
                } != null
            }
            shard.nodes << node
        }

        def targetShard = shards.find { ss ->
            ss.nodes.find { node ->
                node.host == failHostAndPort.host && node.port == failHostAndPort.port
            } != null
        }
        if (!targetShard) {
            log.warn 'failover manager do failover, target shard not found, cluster name={}, fail host and port={}:{}',
                    oneClusterName, failHostAndPort.host, failHostAndPort.port
            return
        }

        def targetMasterNode = targetShard.nodes.find { it.master }
        if (targetMasterNode.host != failHostAndPort.host || targetMasterNode.port != failHostAndPort.port) {
            log.warn 'failover manager do failover, target master node is not fail host and port, cluster name={}, master node={}:{}, fail host and port={}:{}',
                    oneClusterName, targetMasterNode.host, targetMasterNode.port, failHostAndPort.host, failHostAndPort.port
            return
        }

        def targetSlaveNodeList = targetShard.nodes.findAll { !it.master }
        if (!targetSlaveNodeList) {
            log.warn 'failover manager do failover, target slave node is null, cluster name={}, master node={}:{}, fail host and port={}:{}',
                    oneClusterName, targetMasterNode.host, targetMasterNode.port, failHostAndPort.host, failHostAndPort.port
            return
        }

        // choose repl offset nearest slave, todo
        def targetSlaveNode = targetSlaveNodeList.getFirst()

        // change slave to master
        targetSlaveNode.master = true
        targetSlaveNode.followNodeId = null
        // change master to slave, or delete this fail node? todo
        targetMasterNode.master = false
        targetMasterNode.followNodeId = targetSlaveNode.nodeId()

        // clusterx set nodes
        /*
${nodeId} ${ip} ${port} master -
${nodeId} ${ip} ${port} master - 0
${nodeId} ${ip} ${port} master - 0-8191 10000
${nodeId} ${ip} ${port} slave ${primaryNodeId}
       */
        List<String> newLines = []
        shards.each { ss ->
            ss.nodes.each { nn ->
                def line = nn.nodeIdFix + ' ' + nn.host + ' ' + nn.port + ' ' + (nn.master ? 'master' : 'slave') + ' ' + (nn.master ? '-' : nn.followNodeId)
                if (nn.master && ss.multiSlotRange.list) {
                    line += (' ' + ss.multiSlotRange.list.collect { slotRange -> slotRange.toString() }.join(' '))
                }
                newLines << line
            }
        }
        def clusterxNodesArgs = newLines.join('\n')
        log.warn 'cluster name={}, new clusterx args=\n{}', oneClusterName, clusterxNodesArgs

        // broadcast to all nodes
        Set<HostAndPort> allNodeHostAndPortSet = []
        shards.each { ss ->
            ss.nodes.each { nn ->
                allNodeHostAndPortSet << new HostAndPort(nn.host, nn.port)
            }
        }

        for (hostAndPort in allNodeHostAndPortSet) {
            if (hostAndPort == failHostAndPort) {
                // set to zookeeper, loop check if server restart and then set nodes again
                addDelayRestartCheckForFailedHostAndPortNode(failHostAndPort, clusterxNodesArgs)
                continue
            }

            log.warn 'failover manager do failover, cluster name={}, broadcast to host and port={}:{}',
                    oneClusterName, hostAndPort.host, hostAndPort.port
            String result
            if (mockSetNodes) {
                result = 'OK'
            } else {
                def jedisPool = jedisPoolHolder.create(hostAndPort.host, hostAndPort.port)
                result = JedisPoolHolder.exe(jedisPool) { jedis ->
                    def command = new ExtendProtocolCommand('clusterx')
                    byte[] r = jedis.sendCommand(command, 'setnodes'.bytes, clusterxNodesArgs.bytes) as byte[]
                    new String(r)
                }
            }

            if ('OK' != result) {
                log.error 'failover manager clusterx setnodes fail, result={}, target host and port={}:{}',
                        result, hostAndPort.host, hostAndPort.port
                throw new RuntimeException('failover manager clusterx setnodes fail, result=' + result +
                        ', target host and port=' + hostAndPort.host + ':' + hostAndPort.port)
            } else {
                log.warn 'failover manager do failover, cluster name={}, broadcast to host and port={}:{} success',
                        oneClusterName, hostAndPort.host, hostAndPort.port
            }
        }

        updateZookeeperOneMetaAfterDoFailover(oneClusterName, shards)
    }
}
