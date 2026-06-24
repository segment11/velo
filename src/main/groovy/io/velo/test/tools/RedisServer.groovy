package io.velo.test.tools

import org.apache.commons.net.telnet.TelnetClient
import org.slf4j.LoggerFactory
import redis.clients.jedis.Jedis

import java.util.concurrent.TimeUnit

/**
 * Helper for starting and stopping a real {@code redis-server} process during integration tests.
 */
class RedisServer {
    static final String BIN_FILE_PATH = '/usr/local/bin/redis-server'

    private final String name
    private final List<String> args = []

    int port = 6379

    /**
     * @return true if the redis-server binary exists at the expected path
     */
    static boolean isBinExists() {
        new File(BIN_FILE_PATH).exists()
    }

    /**
     * @param name    the logical name of this server instance
     * @param extArgs extra command line arguments forwarded to redis-server
     */
    RedisServer(String name = 'test-redis-server', String... extArgs) {
        this.name = name
        this.args.addAll extArgs
    }

    /**
     * @param port the port to check
     * @param host the host to check
     * @return true if no process is listening on the given host and port
     */
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

    /**
     * @param port the port the redis-server should listen on
     * @return this server instance for method chaining
     */
    RedisServer port(int port) {
        this.port = port

        args << '--port'
        args << "$port".toString()
        this
    }

    /**
     * Picks a random available port and applies it via {@link #port(int)}.
     *
     * @return this server instance for method chaining
     */
    RedisServer randomPort() {
        port(onePortListenAvailable)
    }

    /**
     * Configures the server to disable persistence (no save, no appendonly).
     *
     * @return this server instance for method chaining
     */
    RedisServer noSave() {
        args << '--save'
        args << '""'
        args << '--appendonly'
        args << 'no'
        this
    }

    /**
     * @param dir the working directory for redis data and log files
     * @return this server instance for method chaining
     */
    RedisServer dir(String dir) {
        args << '--dir'
        args << dir
        args << '--logfile'
        args << "${dir}/redis.log".toString()
        this
    }

    /**
     * Configures the server to run as a daemon.
     *
     * @return this server instance for method chaining
     */
    RedisServer daemonize() {
        args << '--daemonize'
        args << 'yes'
        this
    }

    /**
     * @param args additional command line arguments to forward to redis-server
     * @return this server instance for method chaining
     */
    RedisServer args(String... args) {
        this.args.addAll args
        this
    }

    private Process process

    /**
     * Starts the redis-server process and blocks until it exits.
     *
     * @return the process exit code
     */
    int run() {
        def cmd = "$BIN_FILE_PATH ${args.join(' ')}".trim()
        println cmd
        process = ['bash', '-c', cmd].execute()
        def exitCode = process.waitFor()
        exitCode
    }

    /**
     * Stops the redis-server process, escalating to a forced kill after 5 seconds.
     */
    void stop() {
        if (process != null && process.isAlive()) {
            process.destroy()
            println "stop redis server, port=$port"
            // Redis Sentinel can take several seconds to flush its config on SIGTERM;
            // escalate to SIGKILL after 5s so a slow shutdown cannot leak a sentinel
            // process holding the test port across runs.
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                println "redis server did not exit in 5s, force killing, port=$port"
                process.destroyForcibly()
            }
        } else {
            println "redis server not running, port=$port"
        }
    }

    /**
     * Creates a Jedis client and retries until the server is ready.
     *
     * @param host the server host
     * @param port the server port
     * @return a connected Jedis instance
     * @throws RuntimeException if the server is not ready after 50 seconds
     */
    static Jedis initJedis(String host = '127.0.0.1', int port) {
        def log = LoggerFactory.getLogger(RedisServer)
        for (int i = 0; i < 10; i++) {
            try {
                def jedis = new Jedis(host, port)
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

    /**
     * @return a connected Jedis client targeting this server's port
     */
    Jedis jedis() {
        initJedis('127.0.0.1', port)
    }

    /**
     * Entry point for ad-hoc verification that a local redis-server can be started.
     *
     * @param args command line arguments (unused)
     */
    static void main(String[] args) {
        if (!isBinExists()) {
            println 'bin file not exists, please copy redis-server to /usr/local/bin'
            return
        }

        def server = new RedisServer().randomPort().args('--daemonize', 'no')
        Thread.start {
            server.run()
        }
        def jedis = server.jedis()
        jedis.close()
        server.stop()
    }
}
