package io.velo.e2e

import com.fasterxml.jackson.databind.ObjectMapper
import io.velo.repl.cluster.Node
import io.velo.repl.cluster.Shard
import io.velo.repl.cluster.TmpForJson
import io.velo.test.tools.VeloServer
import redis.clients.jedis.util.JedisClusterCRC16
import spock.lang.Specification

import java.net.Socket
import java.nio.charset.StandardCharsets

class ClusterMovedSlotTest extends Specification {
    def 'test moved reply uses redis client slot instead of inner velo slot'() {
        given:
        if (!VeloServer.isJarExists()) {
            println 'velo jar not built, skip'
            expect:
            true
            return
        }

        def suffix = System.nanoTime()
        def serverDir = new File("/tmp/velo-e2e-cluster-moved-${suffix}")
        def remotePort = 47999
        def server = new VeloServer('cluster-moved-slot')
                .randomPort()
                .dir(serverDir.absolutePath)
                .arg('clusterEnabled', true)

        def movedKey = findKeyInSlotRange(8192, 16383)
        def movedSlot = JedisClusterCRC16.getSlot(movedKey.bytes)

        writeNodesJson(serverDir, server.port, remotePort)

        def serverThread = Thread.start {
            server.run()
        }

        and:
        assert waitUntil(30, 1_000L) {
            try {
                def jedis = server.jedis()
                try {
                    return jedis.ping() == 'PONG'
                } finally {
                    jedis.close()
                }
            } catch (Exception ignored) {
                return false
            }
        }

        when:
        def reply = sendRawGet(server.port, movedKey)

        then:
        reply.contains("MOVED ${movedSlot} 127.0.0.1:${remotePort}")

        cleanup:
        server?.stop()
        serverThread?.join(5_000)
        new File("velo-port${server?.port}.properties").delete()
        serverDir?.deleteDir()
    }

    private static void writeNodesJson(File serverDir, int selfPort, int remotePort) {
        def persistDir = new File(serverDir, 'persist')
        persistDir.mkdirs()

        def selfShard = new Shard()
        selfShard.multiSlotRange.addSingle(0, 8191)
        selfShard.nodes << new Node(master: true, host: '127.0.0.1', port: selfPort, mySelf: true)

        def remoteShard = new Shard()
        remoteShard.multiSlotRange.addSingle(8192, 16383)
        remoteShard.nodes << new Node(master: true, host: '127.0.0.1', port: remotePort, mySelf: false)

        def tmp = new TmpForJson(shards: [selfShard, remoteShard], clusterMyEpoch: 1, clusterCurrentEpoch: 1)
        new ObjectMapper().writeValue(new File(persistDir, 'nodes.json'), tmp)
    }

    private static String findKeyInSlotRange(int begin, int end) {
        for (int i = 0; i < 100_000; i++) {
            def key = "cluster:moved:{slot:${i}}"
            def slot = JedisClusterCRC16.getSlot(key.bytes)
            if (slot >= begin && slot <= end) {
                return key
            }
        }
        throw new IllegalStateException("Could not find key for slot range ${begin}-${end}")
    }

    private static String sendRawGet(int port, String key) {
        def socket = new Socket('127.0.0.1', port)
        socket.soTimeout = 5_000
        try {
            def output = socket.getOutputStream()
            output.write(respGet(key))
            output.flush()

            def input = socket.getInputStream()
            def bytes = new ByteArrayOutputStream()
            while (true) {
                def b = input.read()
                if (b == -1) {
                    break
                }
                bytes.write(b)
                def raw = bytes.toString(StandardCharsets.UTF_8)
                if (raw.endsWith("\r\n")) {
                    return raw
                }
            }
            return bytes.toString(StandardCharsets.UTF_8)
        } finally {
            socket.close()
        }
    }

    private static byte[] respGet(String key) {
        return "*2\r\n\$3\r\nGET\r\n\$${key.bytes.length}\r\n${key}\r\n".getBytes(StandardCharsets.UTF_8)
    }

    private static boolean waitUntil(int retryCount = 20, long sleepMillis = 1_000L, Closure<Boolean> condition) {
        for (int i = 0; i < retryCount; i++) {
            if (condition.call()) {
                return true
            }
            Thread.sleep(sleepMillis)
        }
        return false
    }
}
