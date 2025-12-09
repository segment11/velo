package io.velo.tools

import org.apache.commons.net.telnet.TelnetClient
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

class RedisServer {
    static final String BIN_FILE_PATH = '/usr/local/bin/redis-server'

    private final String name
    private final List<String> args = []

    int port = 6379

    static boolean isBinExists() {
        new File(BIN_FILE_PATH).exists()
    }

    RedisServer(String name = 'test-redis-server', String... extArgs) {
        this.name = name
        this.args.addAll extArgs
    }

    static boolean isPortListenAvailable(int port, String host = '127.0.0.1') {
        def tc = new TelnetClient(connectTimeout: 500)
        try {
            tc.connect(host, port)
            return false
        } catch (Exception ignored) {
            return true
        } finally {
            tc.disconnect()
        }
    }

    private static int BEGIN_PORT = 31000

    synchronized static int getOnePortListenAvailable() {
        for (i in (0..<100)) {
            def j = BEGIN_PORT + i
            if (j >= 51000) {
                j = BEGIN_PORT
            }

            if (isPortListenAvailable(j)) {
                BEGIN_PORT = j + 1
                return j
            }
        }
        -1
    }

    RedisServer port(int port) {
        this.port = port

        args << '--port'
        args << "$port".toString()
        this
    }

    RedisServer randomPort() {
        port(onePortListenAvailable)
    }

    RedisServer noSave() {
        args << '--save'
        args << '""'
        args << '--appendonly'
        args << 'no'
        this
    }

    RedisServer args(String... args) {
        this.args.addAll args
        this
    }

    private Process process

    int run() {
        def cmd = "$BIN_FILE_PATH ${args.join(' ')}".trim()
        println cmd
        process = ['bash', '-c', cmd].execute()
        def exitCode = process.waitFor()
        exitCode
    }

    void stop() {
        if (process != null && process.isAlive()) {
            process.destroy()
        }
    }

    Jedis initJedis() {
        def log = LoggerFactory.getLogger(RedisServer)
        for (int i = 0; i < 10; i++) {
            try {
                def jedis = new Jedis('127.0.0.1', port)
                jedis.ping()
                log.info 'connected to server, port={}', port
                return jedis
            } catch (Exception e) {
                log.warn 'waiting for server ready, retry..., {}', e.message
                Thread.sleep(1000 * 5)
            }
        }
        throw new RuntimeException('server not ready after waiting 50 seconds')
    }
}
