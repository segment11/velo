package io.velo.repl.cluster

import io.velo.repl.support.ExtendProtocolCommand
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.util.JedisClusterCRC16
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit

abstract class ClusterxBase extends Specification {
    protected static final String HOST = '127.0.0.1'
    protected static final int SLOT_NUMBER = 4
    protected static final int TOTAL_CLIENT_SLOTS = 16384
    protected static final int STEP = (TOTAL_CLIENT_SLOTS / SLOT_NUMBER) as int
    protected static final int JEDIS_TIMEOUT_MILLIS = 30_000

    protected List<Process> processes = []
    protected File baseDir
    protected List<NodeHandle> nodes
    protected int clusterVersion = 1

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

        // Each run creates a fresh temp base dir with per-node data and logs; delete it to avoid
        // unbounded leaks. Re-run with VELO_CLUSTER_KEEP_DIR=true (or -Dvelo.cluster.keepDir=true) to
        // preserve node dirs and server logs for post-mortem when a scenario fails.
        if (baseDir != null) {
            if (keepDir()) {
                println "Kept cluster dir for inspection: ${baseDir.absolutePath}"
            } else {
                baseDir.deleteDir()
            }
        }
    }

    protected static boolean keepDir() {
        System.getenv('VELO_CLUSTER_KEEP_DIR') == 'true' || System.getProperty('velo.cluster.keepDir') == 'true'
    }

    protected void startCluster() {
        def jar = new File('build/libs/velo-1.0.0.jar')
        assert jar.isFile(): 'Run ./gradlew jar first'

        baseDir = Files.createTempDirectory('velo-cluster-manual-groovy-').toFile()
        println "Cluster base dir: ${baseDir.absolutePath}"
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

    protected void bootstrapInitialTopology() {
        broadcastSetNodes(initialNodesArgs())
        nodes.each { node ->
            assert status(clusterx(node, 'myid')) == node.nodeId
            assert clusterInfo(node).contains('cluster_state:ok')
        }
    }

    protected String initialNodesArgs() {
        [
                "${nodes[0].nodeId} ${HOST} ${nodes[0].port} master - 0-${STEP - 1}",
                "${nodes[1].nodeId} ${HOST} ${nodes[1].port} master - ${STEP}-${(2 * STEP) - 1}",
                "${nodes[2].nodeId} ${HOST} ${nodes[2].port} master - ${2 * STEP}-${TOTAL_CLIENT_SLOTS - 1}",
                "${nodes[3].nodeId} ${HOST} ${nodes[3].port} slave ${nodes[1].nodeId}",
        ].join('\n')
    }

    protected String failoverNodesArgs(boolean afterMigration) {
        def node0Range = afterMigration ? '' : "0-${STEP - 1}"
        def promotedRange = afterMigration ? "0-${(2 * STEP) - 1}" : "${STEP}-${(2 * STEP) - 1}"
        [
                "${nodes[0].nodeId} ${HOST} ${nodes[0].port} master - ${node0Range}".trim(),
                "${nodes[3].nodeId} ${HOST} ${nodes[3].port} master - ${promotedRange}",
                "${nodes[1].nodeId} ${HOST} ${nodes[1].port} slave ${nodes[3].nodeId}",
                "${nodes[2].nodeId} ${HOST} ${nodes[2].port} master - ${2 * STEP}-${TOTAL_CLIENT_SLOTS - 1}",
        ].join('\n')
    }

    protected void broadcastSetNodes(String nodesArgs) {
        clusterVersion++
        nodes.each { node ->
            waitClusterxOk(node, 'setnodes', nodesArgs, clusterVersion.toString())
        }
    }

    protected void broadcastSetSlot(int slot, String nodeId) {
        clusterVersion++
        nodes.each { node ->
            waitClusterxOk(node, 'setslot', slot.toString(), 'node', nodeId, clusterVersion.toString())
        }
    }

    protected static void waitClusterxOk(NodeHandle node, String... args) {
        long deadline = System.currentTimeMillis() + 90_000
        Object last = null
        while (System.currentTimeMillis() < deadline) {
            try {
                last = status(clusterx(node, *args))
                if (last == 'OK') {
                    return
                }
            } catch (Exception e) {
                last = "${e.class.simpleName}: ${e.message}"
            }
            Thread.sleep(1000)
        }
        throw new AssertionError("Timed out waiting for CLUSTERX ${args.join(' ')} on ${node}, last=${last}")
    }

    protected static Object clusterx(NodeHandle node, String... args) {
        node.jedis.sendCommand(new ExtendProtocolCommand('clusterx'), args.collect { it.bytes } as byte[][])
    }

    protected static Object manage(NodeHandle node, String... args) {
        node.jedis.sendCommand(new ExtendProtocolCommand('manage'), args.collect { it.bytes } as byte[][])
    }

    protected static String clusterInfo(NodeHandle node) {
        status(clusterx(node, 'info'))
    }

    protected static String status(Object reply) {
        if (reply instanceof byte[]) {
            return new String(reply as byte[], StandardCharsets.UTF_8)
        }
        return String.valueOf(reply)
    }

    protected static void waitForInfo(NodeHandle node, String expected) {
        long deadline = System.currentTimeMillis() + 90_000
        String last = ''
        while (System.currentTimeMillis() < deadline) {
            last = clusterInfo(node)
            if (last.contains(expected)) {
                return
            }
            Thread.sleep(1000)
        }
        throw new AssertionError("Timed out waiting for ${expected}, last info=${last}")
    }

    protected static void waitForValue(NodeHandle node, String key, String expected) {
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
            Thread.sleep(1000)
        }
        throw new AssertionError("Timed out waiting for ${key}=${expected} on ${node.nodeId}, last=${last}")
    }

    protected static void waitPing(NodeHandle node) {
        long deadline = System.currentTimeMillis() + 90_000
        while (System.currentTimeMillis() < deadline) {
            Jedis jedis = null
            try {
                jedis = new Jedis(HOST, node.port, JEDIS_TIMEOUT_MILLIS)
                if (jedis.ping() == 'PONG') {
                    node.jedis = jedis
                    return
                }
                jedis.close()
            } catch (Exception ignored) {
                jedis?.close()
                Thread.sleep(250)
            }
        }
        throw new AssertionError("Timed out waiting for ${node}")
    }

    protected static int pickPort() {
        def socket = new ServerSocket(0, 1, InetAddress.getByName(HOST))
        try {
            return socket.localPort
        } finally {
            socket.close()
        }
    }

    protected static String findKeyForSlot(int slot) {
        for (int i = 0; i < 200_000; i++) {
            def key = "velo-manual:slot-${slot}:${i}".toString()
            if (JedisClusterCRC16.getSlot(key) == slot) {
                return key
            }
        }
        throw new IllegalStateException("Could not find key for slot ${slot}")
    }

    protected static void movedFrom(NodeHandle node, String key) {
        try {
            node.jedis.get(key)
        } catch (JedisDataException e) {
            assert e.message?.contains('MOVED')
            return
        }
        throw new AssertionError("Expected MOVED from ${node}")
    }

    protected static final class NodeHandle implements Closeable {
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
