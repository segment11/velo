package io.velo.persist

import org.apache.commons.compress.archivers.tar.TarArchiveInputStream
import org.apache.commons.compress.compressors.gzip.GzipCompressorInputStream
import org.apache.commons.net.telnet.TelnetClient

class Consts {
    static final File persistDir = new File('/tmp/velo-data/test-persist/')
    static final File slotDir = new File(persistDir, 'test-slot')
    static final File slotDir2 = new File(persistDir, 'test-slot2')
    static final File testDir = new File('/tmp/test-velo-data/')
    static final String zookeeperVersion = '3.8.6'
    static final File zookeeperWorkDir = new File('/tmp/velo-zookeeper')
    static final File zookeeperArchiveFile = new File('/tmp', "apache-zookeeper-${zookeeperVersion}-bin.tar.gz")
    static final File zookeeperExtractDir = new File(zookeeperWorkDir, "apache-zookeeper-${zookeeperVersion}-bin")
    static final File zookeeperDataDir = new File(zookeeperWorkDir, 'data')
    static final File zookeeperLogDir = new File(zookeeperWorkDir, 'logs')
    static final File zookeeperPidFile = new File(zookeeperWorkDir, 'zookeeper.pid')
    static final File zookeeperOutFile = new File(zookeeperWorkDir, 'zookeeper.out')

    static {
        slotDir.mkdirs()
    }

    static boolean checkConnectAvailable(String host = 'localhost', int port = 2181) {
        def tc = new TelnetClient(connectTimeout: 500)
        try {
            tc.connect(host, port)
            return true
        } catch (Exception ignored) {
            return false
        } finally {
            tc.disconnect()
        }
    }

    static String zookeeperDownloadUrl() {
        return "https://dlcdn.apache.org/zookeeper/zookeeper-${zookeeperVersion}/apache-zookeeper-${zookeeperVersion}-bin.tar.gz"
    }

    static void stopZookeeper() {
        if (zookeeperPidFile.exists()) {
            def pid = zookeeperPidFile.text.trim()
            if (pid) {
                def handle = ProcessHandle.of(Long.parseLong(pid))
                if (handle.present && handle.get().alive) {
                    handle.get().destroy()
                    println 'Stopped zookeeper process'
                }
            }
        } else {
            println 'Zookeeper process not found'
        }
    }

    static boolean downloadAndStartZookeeper(String host = 'localhost', int port = 2181) {
        if (checkConnectAvailable(host, port)) {
            println 'Zookeeper already started'
            Consts.zookeeperWorkDir.mkdir()
            return true
        }

        zookeeperWorkDir.mkdirs()
        zookeeperDataDir.mkdirs()
        zookeeperLogDir.mkdirs()

        downloadZookeeperIfNeed()
        extractZookeeperIfNeed()
        writeZookeeperConfig(host, port)
        startZookeeperProcess()

        for (int i = 0; i < 40; i++) {
            if (checkConnectAvailable(host, port)) {
                return true
            }
            Thread.sleep(500)
        }
        return false
    }

    private static void downloadZookeeperIfNeed() {
        println 'Downloading ZooKeeper...'
        println zookeeperDownloadUrl()
        if (zookeeperArchiveFile.exists() && zookeeperArchiveFile.length() > 0) {
            return
        }

        def connection = new URI(zookeeperDownloadUrl()).toURL().openConnection()
        connection.connectTimeout = 10_000
        connection.readTimeout = 60_000
        connection.inputStream.withCloseable { input ->
            zookeeperArchiveFile.withOutputStream { output ->
                input.transferTo(output)
            }
        }
    }

    private static void extractZookeeperIfNeed() {
        if (new File(zookeeperExtractDir, 'bin/zkServer.sh').exists()) {
            return
        }

        zookeeperExtractDir.deleteDir()
        zookeeperWorkDir.mkdirs()

        zookeeperArchiveFile.withInputStream { fileInput ->
            def gzipInput = new GzipCompressorInputStream(fileInput)
            def tarInput = new TarArchiveInputStream(gzipInput)
            tarInput.withCloseable { input ->
                def entry
                while ((entry = input.nextEntry) != null) {
                    def target = new File(zookeeperWorkDir, entry.name)
                    if (entry.directory) {
                        target.mkdirs()
                        continue
                    }

                    target.parentFile?.mkdirs()
                    target.withOutputStream { output ->
                        input.transferTo(output)
                    }
                    if ((entry.mode & 0100) != 0) {
                        target.setExecutable(true, true)
                    }
                }
            }
        }
    }

    private static void writeZookeeperConfig(String host, int port) {
        def confDir = new File(zookeeperExtractDir, 'conf')
        confDir.mkdirs()

        def cfg = new File(confDir, 'zoo.cfg')
        cfg.text = """tickTime=2000
dataDir=${zookeeperDataDir.absolutePath}
clientPort=${port}
clientPortAddress=${host}
initLimit=5
syncLimit=2
admin.enableServer=false
4lw.commands.whitelist=ruok,mntr,conf,isro
"""
    }

    private static void startZookeeperProcess() {
        println 'Starting ZooKeeper...'
        def zkServer = new File(zookeeperExtractDir, 'bin/zkServer.sh')
        if (!zkServer.exists()) {
            throw new IllegalStateException("ZooKeeper start script not found: ${zkServer.absolutePath}")
        }

        if (zookeeperPidFile.exists()) {
            try {
                def pid = zookeeperPidFile.text.trim()
                if (pid) {
                    def handle = ProcessHandle.of(Long.parseLong(pid))
                    if (handle.present && handle.get().alive) {
                        return
                    }
                }
            } catch (Exception ignored) {
            }
        }

        def process = new ProcessBuilder(
                '/bin/bash',
                '-lc',
                "ZOO_LOG_DIR='${zookeeperLogDir.absolutePath}' ZOO_PID_FILE='${zookeeperPidFile.absolutePath}' '${zkServer.absolutePath}' start"
        )
                .redirectErrorStream(true)
                .redirectOutput(ProcessBuilder.Redirect.appendTo(zookeeperOutFile))
                .start()

        if (process.waitFor() != 0) {
            throw new IllegalStateException("Start ZooKeeper failed, see ${zookeeperOutFile.absolutePath}")
        }
    }
}
