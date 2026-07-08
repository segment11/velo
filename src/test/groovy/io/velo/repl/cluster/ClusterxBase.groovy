package io.velo.repl.cluster

import io.velo.repl.support.ExtendProtocolCommand
import redis.clients.jedis.Jedis
import redis.clients.jedis.exceptions.JedisDataException
import redis.clients.jedis.util.JedisClusterCRC16
import spock.lang.Specification

import java.nio.charset.StandardCharsets
import java.nio.file.Files
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicBoolean

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
bigStringNoMemoryCopySize=4096
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

    /**
     * Runs the full migrate sequence for one client slot from {@code from} to {@code to}:
     * import on the target, export on the source, wait for catch-up, then broadcast the ownership cutover.
     */
    protected void migrateSlot(int clientSlot, NodeHandle from, NodeHandle to, List<NodeHandle> toReplicas = []) {
        assert status(manage(to, 'slot', clientSlot.toString(), 'migrate_from', HOST, from.port.toString(), 'force')) == 'OK'
        assert status(clusterx(from, 'migrate', clientSlot.toString(), to.nodeId)) == 'OK'
        waitForInfo(from, 'migrating_state:success')

        // pre-cutover downstream resync: trigger the new owner's replicas to fetch the now-populated slot from it,
        // so they are synced before cutover (closes the no-replica durability window)
        toReplicas.each { r ->
            assert status(manage(r, 'slot', clientSlot.toString(), 'migrate_from', HOST, to.port.toString(), 'force')) == 'OK'
        }

        broadcastSetSlotChargeFirst(clientSlot, to, from)
    }

    /**
     * Broadcasts the ownership cutover with the new owner first (its setslot defers until its replicas are
     * caught up) and the source last (so it flushes its copy only after the new owner has promoted with a
     * synced replica).
     */
    protected void broadcastSetSlotChargeFirst(int clientSlot, NodeHandle to, NodeHandle from) {
        clusterVersion++
        def ordered = [to] + (nodes - [to, from]) + [from]
        ordered.each { node ->
            waitClusterxOk(node, 'setslot', clientSlot.toString(), 'node', to.nodeId, clusterVersion.toString())
        }
    }

    /**
     * Any key that maps to {@code slot} doubles as a hash tag: {@code {tag}suffix} routes by CRC16(tag),
     * so all keys sharing the tag land in the same slot.
     */
    protected static String hashTagForSlot(int slot) {
        findKeyForSlot(slot)
    }

    /**
     * Spawns a daemon thread that SETs {@code {hashTag}:w:i} keys (all in the tag's slot) on its own
     * Jedis connection until {@code stop} is set, recording each acked (key,value) into {@code acked}.
     * Writes that hit MOVED/readonly during cutover are skipped, so {@code acked} holds only durable writes.
     */
    protected Thread startBackgroundWriter(NodeHandle node, String hashTag, AtomicBoolean stop,
                                           List<List<String>> acked, int throttleMillis) {
        def writerJedis = new Jedis(HOST, node.port, JEDIS_TIMEOUT_MILLIS)
        def t = new Thread({
            try {
                int i = 0
                while (!stop.get()) {
                    def key = '{' + hashTag + '}:w:' + i
                    def val = 'wv-' + i
                    try {
                        if (writerJedis.set(key, val) == 'OK') {
                            synchronized (acked) {
                                acked << [key, val]
                            }
                        }
                    } catch (Exception ignored) {
                        // MOVED / readonly during cutover — only acked writes are recorded
                    }
                    i++
                    if (throttleMillis > 0) {
                        Thread.sleep(throttleMillis)
                    }
                }
            } finally {
                writerJedis.close()
            }
        })
        t.daemon = true
        t.start()
        t
    }

    /**
     * Builds a deterministic value of roughly {@code approxKb} KiB, large enough to exceed the
     * configured big-string threshold so the value takes the big-string replication path.
     */
    protected static String bigValue(int approxKb) {
        def target = approxKb * 1024
        def sb = new StringBuilder(target + 40)
        def chunk = 'velo-big-string-payload-0123456789-'
        while (sb.length() < target) {
            sb.append(chunk)
        }
        sb.toString()
    }

    /**
     * Recursively counts big-string files stored under a node's data dir (any {@code big-string}
     * subdirectory), to confirm a value actually took the big-string replication path.
     */
    protected long bigStringFileCount(int nodeIndex) {
        def dataDir = new File(new File(baseDir, "node-${nodeIndex}"), 'data')
        if (!dataDir.exists()) {
            return 0L
        }
        long count = 0L
        dataDir.eachDirRecurse { d ->
            if (d.name == 'big-string') {
                d.eachFileRecurse { f ->
                    if (f.isFile()) {
                        count++
                    }
                }
            }
        }
        count
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

    /**
     * True if node {@code nodeIndex}'s server.log contains {@code text}. Used to assert that a code path
     * that leaves no client-visible signal actually ran end-to-end (e.g. "Released shed one slot=2").
     */
    protected boolean serverLogContains(int nodeIndex, String text) {
        def log = new File(new File(baseDir, "node-${nodeIndex}"), 'server.log')
        log.isFile() && log.text.contains(text)
    }

    /**
     * Polls node {@code nodeIndex}'s server.log until it contains {@code text} or the timeout elapses.
     * Returns true on match. Server logs are written asynchronously, so a just-triggered message may not
     * be flushed the instant the client command returns.
     */
    protected boolean waitServerLogContains(int nodeIndex, String text, long timeoutMillis = 30_000) {
        long deadline = System.currentTimeMillis() + timeoutMillis
        while (System.currentTimeMillis() < deadline) {
            if (serverLogContains(nodeIndex, text)) {
                return true
            }
            Thread.sleep(500)
        }
        false
    }

    /** Number of non-overlapping occurrences of {@code text} in node {@code nodeIndex}'s server.log. */
    protected int countServerLog(int nodeIndex, String text) {
        def log = new File(new File(baseDir, "node-${nodeIndex}"), 'server.log')
        if (!log.isFile()) {
            return 0
        }
        int count = 0, from = 0
        def content = log.text
        while (true) {
            int idx = content.indexOf(text, from)
            if (idx < 0) {
                break
            }
            count++
            from = idx + text.length()
        }
        count
    }

    /**
     * Number of open file descriptors held by node {@code nodeIndex}'s JVM process, read from
     * {@code /proc/<pid>/fd} (Linux). Returns -1 if unavailable. Used to detect fd leaks across a
     * slot init/release/re-init cycle.
     */
    protected int openFdCount(int nodeIndex) {
        def pid = processes[nodeIndex].pid()
        def fdDir = new File("/proc/${pid}/fd")
        def names = fdDir.list()
        names == null ? -1 : names.length
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
