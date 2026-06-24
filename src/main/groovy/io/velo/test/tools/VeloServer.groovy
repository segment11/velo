package io.velo.test.tools

import com.segment.common.Conf
import redis.clients.jedis.Jedis

import java.util.concurrent.TimeUnit

/**
 * Helper for starting and stopping a real Velo server process during integration tests.
 */
class VeloServer {
    static final String jdkVersion
    static final String JAVA_BIN_PATH
    static String BUILD_LIBS_DIR_PATH = "build/libs"
    static String JAR_FILE_PATH = "build/libs/velo-1.0.0.jar"

    static {
        jdkVersion = System.getProperty('java.version')
        println "jdk version: $jdkVersion"
        // check if jdk version is >= 21
        if (jdkVersion.startsWith('1.')) {
            throw new RuntimeException("jdk version must >= 21, current version is $jdkVersion")
        } else {
            def arr = jdkVersion.split(/\./)
            def bigVersion = arr[0] as int
            if (bigVersion < 21) {
                throw new RuntimeException("jdk version must >= 21, current version is $jdkVersion")
            }
        }

        def c = Conf.instance
        c.resetWorkDir(true)
        JAVA_BIN_PATH = new File(System.getProperty('java.home'), 'bin/java').absolutePath
        def buildGradleFilePath = c.projectPath('/build.gradle')
        def veloVersion = new File(buildGradleFilePath).text.readLines().find { line -> line.startsWith('version =') }
                .split('=')[1].trim().replaceAll("'", '')
        BUILD_LIBS_DIR_PATH = c.projectPath('/build/libs')
        JAR_FILE_PATH = "${BUILD_LIBS_DIR_PATH}/velo-${veloVersion}.jar"
    }

    private final String name
    private final List<String> args = []

    int port = 7379
    String dir

    /**
     * @return true if the built Velo jar exists at the expected path
     */
    static boolean isJarExists() {
        new File(JAR_FILE_PATH).exists()
    }

    /**
     * @param name    the logical name of this server instance
     * @param extArgs extra key=value arguments forwarded to the Velo server
     */
    VeloServer(String name = 'test-velo-server', String... extArgs) {
        this.name = name
        this.args.addAll extArgs
    }

    private static int BEGIN_PORT = 41000

    synchronized static int getOnePortListenAvailable() {
        for (i in (0..<100)) {
            def j = BEGIN_PORT + i
            if (j >= 51000) {
                j = BEGIN_PORT
            }

            if (RedisServer.isPortListenAvailable(j)) {
                BEGIN_PORT = j + 1
                return j
            }
        }
        -1
    }

    /**
     * @param port the port the Velo server should listen on
     * @return this server instance for method chaining
     */
    VeloServer port(int port) {
        this.port = port
        this
    }

    /**
     * Picks a random available port and applies it via {@link #port(int)}.
     *
     * @return this server instance for method chaining
     */
    VeloServer randomPort() {
        port(onePortListenAvailable)
    }

    /**
     * @param dir the working directory for Velo data files
     * @return this server instance for method chaining
     */
    VeloServer dir(String dir) {
        this.dir = dir
        this
    }

    /**
     * @param key   the configuration key
     * @param value the configuration value
     * @return this server instance for method chaining
     */
    VeloServer arg(String key, Object value) {
        this.args.add "${key}=${value.toString()}".toString()
        this
    }

    private Process process

    /**
     * Generates a temporary properties file, starts the Velo server process, and blocks until it exits.
     *
     * @return the process exit code
     */
    int run() {
        def tmpConfigPath = "velo-port${port}.properties"
        new File(tmpConfigPath).text = """
slotNumber=1
slotWorkers=1
netWorkers=1
indexWorkers=1
dir=${dir ?: '/tmp/velo-data-port-' + port}
net.listenAddresses=0.0.0.0:${port}

${args ? args.join("\r\n") : ''}
"""
        def cmd = "\"$JAVA_BIN_PATH\" -jar \"$JAR_FILE_PATH\" ${tmpConfigPath}".trim()
        println cmd
        process = ['bash', '-c', cmd].execute()
        process.consumeProcessOutput(System.out, System.err)
        def exitCode = process.waitFor()
        exitCode
    }

    /**
     * Stops the Velo server process, escalating to a forced kill after 5 seconds.
     */
    void stop() {
        if (process != null && process.isAlive()) {
            process.destroy()
            println "stop velo server, port=$port"
            // Give ActiveJ up to 5s to run shutdown hooks on SIGTERM, then escalate to
            // SIGKILL so a hung or slow shutdown cannot leak a JVM holding the test port
            // (which would cause the next test run to randomly pick a different port and
            // potentially auto-discover the leaked instance).
            if (!process.waitFor(5, TimeUnit.SECONDS)) {
                println "velo server did not exit in 5s, force killing, port=$port"
                process.destroyForcibly()
            }
        } else {
            println "velo server not running, port=$port"
        }
    }

    /**
     * @return a connected Jedis client targeting this server's port
     */
    Jedis jedis() {
        RedisServer.initJedis('127.0.0.1', port)
    }

    /**
     * Entry point for ad-hoc verification that the built Velo jar can be started.
     *
     * @param args command line arguments (unused)
     */
    static void main(String[] args) {
        if (!isJarExists()) {
            println 'jar file not exists, please run `./gradlew jar` first'
            return
        }

        def server = new VeloServer().randomPort().arg('estimateKeyNumber', 10000000)
        Thread.start {
            server.run()
        }
        def jedis = server.jedis()
        jedis.close()
        server.stop()
    }
}
