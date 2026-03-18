package io.velo.test.tools

import com.segment.common.Conf
import redis.clients.jedis.Jedis

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

    static boolean isJarExists() {
        new File(JAR_FILE_PATH).exists()
    }

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

    VeloServer port(int port) {
        this.port = port
        this
    }

    VeloServer randomPort() {
        port(onePortListenAvailable)
    }

    VeloServer dir(String dir) {
        this.dir = dir
        this
    }

    VeloServer arg(String key, Object value) {
        this.args.add "${key}=${value.toString()}".toString()
        this
    }

    private Process process

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

    void stop() {
        if (process != null && process.isAlive()) {
            process.destroy()
            println "stop velo server, port=$port"
        } else {
            println "velo server not running, port=$port"
        }
    }

    Jedis jedis() {
        RedisServer.initJedis('127.0.0.1', port)
    }

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
