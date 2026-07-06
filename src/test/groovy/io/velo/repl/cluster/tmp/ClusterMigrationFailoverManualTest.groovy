package io.velo.repl.cluster.tmp

import io.velo.repl.support.ExtendProtocolCommand
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.util.JedisClusterCRC16
import spock.lang.IgnoreIf
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit

/**
 * Opt-in manual reproducer for cluster migration/failover data-loss investigation.
 *
 * Run only when explicitly requested:
 *   VELO_CLUSTER_MANUAL=true ./gradlew :test --tests "io.velo.repl.cluster.tmp.ClusterMigrationFailoverManualTest" --rerun-tasks
 */
@IgnoreIf({ System.getenv('VELO_CLUSTER_MANUAL') != 'true' && System.getProperty('velo.cluster.manual') != 'true' })
class ClusterMigrationFailoverManualTest extends Specification {
    private static final String HOST = '127.0.0.1'
    private static final int SLOT_NUMBER = 4
    private static final int TOTAL_CLIENT_SLOTS = 16384
    private static final int STEP = (TOTAL_CLIENT_SLOTS / SLOT_NUMBER) as int

    List<Process> processes = []
    File baseDir
    List<NodeHandle> nodes
    int clusterVersion = 1

    def cleanup() {
        processes.each { process ->
            if (process.isAlive()) {
                process.destroy()
            }
        }
        processes.each { process ->
            if (process.isAlive()) {
                process.waitFor(2, TimeUnit.SECONDS)
            }
            if (process.isAlive()) {
                process.destroyForcibly()
            }
        }
        nodes?.each { it.close() }
    }

    def 'manual repro: slot migration should preserve data on the new owner'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        def key = findKeyForSlot(0)
        assert nodes[0].jedis.set(key, 'before-migrate') == 'OK'
        assert nodes[0].jedis.get(key) == 'before-migrate'

        when:
        assert status(manage(nodes[1], 'slot', '0', 'migrate_from', HOST, nodes[0].port.toString(), 'force')) == 'OK'
        assert status(clusterx(nodes[0], 'migrate', '0', nodes[1].nodeId)) == 'OK'
        waitForInfo(nodes[0], 'migrating_state:success')
        broadcastSetSlot(0, nodes[1].nodeId)

        then:
        movedFrom(nodes[0], key)
        nodes[1].jedis.get(key) == 'before-migrate'
        waitForValue(nodes[3], key, 'before-migrate')
    }

    def 'manual repro: forced failover should preserve replicated data on promoted replica'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        def key = findKeyForSlot(STEP)
        assert nodes[1].jedis.set(key, 'before-failover') == 'OK'
        waitForValue(nodes[3], key, 'before-failover')

        when:
        assert status(clusterx(nodes[3], 'failover', 'force')) == 'OK'
        broadcastSetNodes(failoverNodesArgs(false))

        then:
        clusterInfo(nodes[3]).contains('cluster_state:ok')
        nodes[3].jedis.get(key) == 'before-failover'
    }

    private void startCluster() {
        def jar = new File('build/libs/velo-1.0.0.jar')
        assert jar.isFile(): 'Run ./gradlew jar first'

        baseDir = Files.createTempDirectory('velo-cluster-manual-groovy-').toFile()
        def ports = (0..<4).collect { pickPort() }
        nodes = ports.withIndex().collect { int port, int i ->
            new NodeHandle(i, "velo-manual-${i}".toString(), port)
        }

        nodes.each { node ->
            def nodeDir = new File(baseDir, "node-${node.index}")
            assert nodeDir.mkdirs()
            def props = new File(nodeDir, 'velo.properties')
            props.text = """
slotNumber=${SLOT_NUMBER}
slotWorkers=1
netWorkers=1
dir=${new File(nodeDir, 'data').absolutePath}
net.listenAddresses=0.0.0.0:${node.port}
replicaAnnounceIp=${HOST}
replicaAnnouncePort=${node.port}
estimateKeyNumber=1000000
kv.lru.maxSize=100000
persist.binlogOn=true
clusterEnabled=true
""".stripIndent().trim() + '\n'

            def log = new File(nodeDir, 'server.log')
            def process = new ProcessBuilder(
                    'java',
                    '-XX:+UseZGC',
                    '-XX:+ZGenerational',
                    '-XX:MaxDirectMemorySize=64m',
                    '-Xmx512m',
                    '-Xms512m',
                    '-jar',
                    jar.absolutePath,
                    props.absolutePath
            ).directory(new File('.'))
                    .redirectOutput(ProcessBuilder.Redirect.appendTo(log))
                    .redirectError(ProcessBuilder.Redirect.appendTo(log))
                    .start()
            processes << process
        }

        nodes.each { node ->
            waitPing(node)
        }
    }

    private void bootstrapInitialTopology() {
        broadcastSetNodes(initialNodesArgs())
        nodes.each { node ->
            assert status(clusterx(node, 'myid')) == node.nodeId
            assert clusterInfo(node).contains('cluster_state:ok')
        }
    }

    private String initialNodesArgs() {
        [
                "${nodes[0].nodeId} ${HOST} ${nodes[0].port} master - 0-${STEP - 1}",
                "${nodes[1].nodeId} ${HOST} ${nodes[1].port} master - ${STEP}-${(2 * STEP) - 1}",
                "${nodes[2].nodeId} ${HOST} ${nodes[2].port} master - ${2 * STEP}-${TOTAL_CLIENT_SLOTS - 1}",
                "${nodes[3].nodeId} ${HOST} ${nodes[3].port} slave ${nodes[1].nodeId}",
        ].join('\n')
    }

    private String failoverNodesArgs(boolean afterMigration) {
        def node0Range = afterMigration ? '' : "0-${STEP - 1}"
        def promotedRange = afterMigration ? "0-${(2 * STEP) - 1}" : "${STEP}-${(2 * STEP) - 1}"
        [
                "${nodes[0].nodeId} ${HOST} ${nodes[0].port} master - ${node0Range}".trim(),
                "${nodes[3].nodeId} ${HOST} ${nodes[3].port} master - ${promotedRange}",
                "${nodes[1].nodeId} ${HOST} ${nodes[1].port} slave ${nodes[3].nodeId}",
                "${nodes[2].nodeId} ${HOST} ${nodes[2].port} master - ${2 * STEP}-${TOTAL_CLIENT_SLOTS - 1}",
        ].join('\n')
    }

    private void broadcastSetNodes(String nodesArgs) {
        clusterVersion++
        nodes.each { node ->
            assert status(clusterx(node, 'setnodes', nodesArgs, clusterVersion.toString())) == 'OK'
        }
    }

    private void broadcastSetSlot(int slot, String nodeId) {
        clusterVersion++
        nodes.each { node ->
            assert status(clusterx(node, 'setslot', slot.toString(), 'node', nodeId, clusterVersion.toString())) == 'OK'
        }
    }

    private static Object clusterx(NodeHandle node, String... args) {
        node.jedis.sendCommand(new ExtendProtocolCommand('clusterx'), args.collect { it.bytes } as byte[][])
    }

    private static Object manage(NodeHandle node, String... args) {
        node.jedis.sendCommand(new ExtendProtocolCommand('manage'), args.collect { it.bytes } as byte[][])
    }

    private static String clusterInfo(NodeHandle node) {
        status(clusterx(node, 'info'))
    }

    private static String status(Object reply) {
        if (reply instanceof byte[]) {
            return new String(reply as byte[], StandardCharsets.UTF_8)
        }
        return String.valueOf(reply)
    }

    private static void waitForInfo(NodeHandle node, String expected) {
        long deadline = System.currentTimeMillis() + 90_000
        String last = ''
        while (System.currentTimeMillis() < deadline) {
            last = clusterInfo(node)
            if (last.contains(expected)) {
                return
            }
            sleep(1000)
        }
        throw new AssertionError("Timed out waiting for ${expected}, last info=${last}")
    }

    private static void waitForValue(NodeHandle node, String key, String expected) {
        long deadline = System.currentTimeMillis() + 90_000
        Object last = null
        while (System.currentTimeMillis() < deadline) {
            try {
                last = node.jedis.get(key)
                if (last == expected) {
                    return
                }
            } catch (Exception e) {
                last = "${e.class.simpleName}: ${e.message}"
            }
            sleep(1000)
        }
        throw new AssertionError("Timed out waiting for ${key}=${expected} on ${node.nodeId}, last=${last}")
    }

    private static void waitPing(NodeHandle node) {
        long deadline = System.currentTimeMillis() + 90_000
        while (System.currentTimeMillis() < deadline) {
            try {
                def jedis = new Jedis(HOST, node.port)
                if (jedis.ping() == 'PONG') {
                    node.jedis = jedis
                    return
                }
                jedis.close()
            } catch (Exception ignored) {
                sleep(250)
            }
        }
        throw new AssertionError("Timed out waiting for ${node}")
    }

    private static int pickPort() {
        def socket = new ServerSocket(0, 1, InetAddress.getByName(HOST))
        try {
            return socket.localPort
        } finally {
            socket.close()
        }
    }

    private static String findKeyForSlot(int slot) {
        for (int i = 0; i < 200_000; i++) {
            def key = "velo-manual:slot-${slot}:${i}".toString()
            if (JedisClusterCRC16.getSlot(key) == slot) {
                return key
            }
        }
        throw new IllegalStateException("Could not find key for slot ${slot}")
    }

    private static void movedFrom(NodeHandle node, String key) {
        try {
            node.jedis.get(key)
        } catch (JedisDataException e) {
            assert e.message?.contains('MOVED')
            return
        }
        throw new AssertionError("Expected MOVED from ${node}")
    }

    private static final class NodeHandle implements Closeable {
        final int index
        final String nodeId
        final int port
        Jedis jedis

        NodeHandle(int index, String nodeId, int port) {
            this.index = index
            this.nodeId = nodeId
            this.port = port
        }

        @Override
        void close() {
            jedis?.close()
        }

        @Override
        String toString() {
            return "${nodeId}@${HOST}:${port}"
        }
    }
}
