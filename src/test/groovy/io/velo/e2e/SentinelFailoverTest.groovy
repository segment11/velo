package io.velo.e2e

import io.velo.test.tools.RedisServer
import io.velo.test.tools.VeloServer
import redis.clients.jedis.Jedis
import spock.lang.Specification

/**
 * Black-box auto-failover test driven by a real Redis Sentinel process pool.
 *
 * <p>Topology:
 * <pre>
 *   Velo master (sentinelModeEnabled=true)
 *   ├── Velo replica 1 (SLAVEOF master)
 *   └── Velo replica 2 (SLAVEOF master)
 *
 *   Redis Sentinel x3  (monitor mymaster, quorum=2)
 * </pre>
 *
 * The test drives the full Sentinel failover workflow end-to-end:
 * <ol>
 *   <li>start Velo master + 2 replicas, wire replication via {@code SLAVEOF}</li>
 *   <li>start 3 real {@code redis-server --sentinel} processes monitoring the master</li>
 *   <li>wait until Sentinel discovers the replicas through {@code INFO replication}</li>
 *   <li>kill the Velo master process</li>
 *   <li>wait for Sentinel to elect and promote a replica via {@code SLAVEOF NO ONE}</li>
 *   <li>verify the promoted Velo node now reports {@code role:master}</li>
 *   <li>verify the surviving replica follows the new master through {@code SLAVEOF host port}</li>
 *   <li>write to the new master and assert the data reaches the surviving replica</li>
 * </ol>
 *
 * Requires the Velo fat jar ({@code build/libs/velo-1.0.0.jar}) and the
 * {@code redis-server} binary on the test host. Skips gracefully when either is absent.
 */
class SentinelFailoverTest extends Specification {
    static final String MASTER_NAME = 'mymaster'

    def 'test sentinel auto failover'() {
        given:
        if (!VeloServer.isJarExists()) {
            println 'velo jar not built, skip'
            expect: true
            return
        }
        if (!RedisServer.isBinExists()) {
            println 'redis-server bin not exists, skip'
            expect: true
            return
        }

        def suffix = System.nanoTime()
        def baseDir = "/tmp/velo-sentinel-e2e-${suffix}"
        new File(baseDir).mkdirs()

        VeloServer master = null
        VeloServer replica1 = null
        VeloServer replica2 = null
        Thread masterThread = null
        Thread replica1Thread = null
        Thread replica2Thread = null

        List<RedisServer> sentinels = []
        List<Thread> sentinelThreads = []

        Jedis masterJedis = null
        Jedis replica1Jedis = null
        Jedis replica2Jedis = null
        Jedis sentinelJedis = null

        // Stable announce IP so Sentinel can reconfigure replicas onto the new master.
        String announceIp = '127.0.0.1'

        // Make the master eligible for Sentinel discovery before any replica is wired.
        master = newVeloServer('sentinel-master', "${baseDir}/master")
        replica1 = newVeloServer('sentinel-replica1', "${baseDir}/replica1")
        replica2 = newVeloServer('sentinel-replica2', "${baseDir}/replica2")

        master.arg('replicaAnnouncePort', master.port)
        replica1.arg('replicaAnnouncePort', replica1.port)
        replica2.arg('replicaAnnouncePort', replica2.port)

        masterThread = Thread.start { master.run() }
        // Stagger replica starts by a beat so their bind does not race with the master's
        // JVM init for the OS port-allocation lock; e2e runs that share the host with
        // prior tests sometimes hit transient BindException otherwise.
        Thread.sleep(2_000L)
        replica1Thread = Thread.start { replica1.run() }
        Thread.sleep(2_000L)
        replica2Thread = Thread.start { replica2.run() }

        and:
        masterJedis = master.jedis()
        replica1Jedis = replica1.jedis()
        replica2Jedis = replica2.jedis()

        // Sentinel needs at least one key in the binlog to compute a real master offset;
        // write one before wiring replicas so INFO replication reports a healthy master.
        masterJedis.set('repl:seed:before', 'hello')

        when:
        // Wire both replicas to the master using Sentinel-era command.
        def r1 = replica1Jedis.slaveof(announceIp, master.port)
        def r2 = replica2Jedis.slaveof(announceIp, master.port)

        then:
        r1 == 'OK'
        r2 == 'OK'
        waitUntil(60, 1_000L) {
            try {
                def m = masterJedis.info('replication')
                def r1Info = replica1Jedis.info('replication')
                def r2Info = replica2Jedis.info('replication')
                return m.contains('role:master') &&
                        m.contains('connected_slaves:2') &&
                        r1Info.contains('role:slave') &&
                        r1Info.contains("master_host:${announceIp}") &&
                        r1Info.contains("master_port:${master.port}") &&
                        r1Info.contains('master_link_status:up') &&
                        r2Info.contains('role:slave') &&
                        r2Info.contains("master_port:${master.port}") &&
                        r2Info.contains('master_link_status:up') &&
                        replica1Jedis.get('repl:seed:before') == 'hello' &&
                        replica2Jedis.get('repl:seed:before') == 'hello'
            } catch (Exception ignored) {
                return false
            }
        }

        when:
        // Start 3 real Redis Sentinels monitoring the Velo master.
        def sentinelResult = startSentinels(baseDir, announceIp, master.port, 3)
        sentinels = sentinelResult[0] as List<RedisServer>
        sentinelThreads = sentinelResult[1] as List<Thread>
        sentinelJedis = RedisServer.initJedis('127.0.0.1', sentinels[0].port)

        then:
        // Sentinel reports the master at the address we configured.
        waitUntil(40, 1_000L) {
            try {
                def addr = sentinelJedis.sentinelGetMasterAddrByName(MASTER_NAME)
                return addr != null && addr.size() == 2 && addr[1] == "${master.port}"
            } catch (Exception ignored) {
                return false
            }
        }

        when:
        // Sentinel probes replicas through INFO replication; allow time for discovery.
        // Jedis sentinelReplicas() returns maps whose keys are Redis Sentinel reply fields:
        // 'flags' (e.g. 'slave'), 'role-reported' (e.g. 'slave'), 'ip', 'port', etc. There is
        // no bare 'role' key on a replica entry.
        def replicasDiscovered = waitUntil(60, 1_000L) {
            try {
                def list = sentinelJedis.sentinelReplicas(MASTER_NAME)
                if (list == null || list.size() < 2) {
                    return false
                }
                return list.count {
                    ('slave' in (it.flags ?: '').split(',')) || 'slave' == it.'role-reported'
                } >= 2
            } catch (Exception ignored) {
                return false
            }
        }

        then:
        replicasDiscovered

        when:
        // Record initial master port, then kill the master to trigger auto-failover.
        def oldMasterPort = master.port
        master.stop()
        masterThread.join(10_000)

        then:
        !masterThread.alive

        when:
        // Sentinel elects a new master after down-after-milliseconds + failover handover.
        // Sentinel's reported master address can flap during the election; instead of trusting
        // that view, poll each Velo replica directly until exactly one reports role:master.
        // That node is the promoted one; the other must follow it.
        println "[sentinel-test] waiting for promoted replica to report role:master, " +
                "replica1=${replica1.port}, replica2=${replica2.port}"
        def promotedPort = waitForPromotedReplica(replica1.port, replica2.port, 90)

        then:
        promotedPort > 0
        promotedPort == replica1.port || promotedPort == replica2.port

        when:
        int followerPort = (promotedPort == replica1.port) ? replica2.port : replica1.port
        println "[sentinel-test] promoted port=${promotedPort}, follower port=${followerPort}"

        and:
        // Sentinel issues CLIENT KILL TYPE normal during failover, which invalidates long-lived
        // Jedis handles opened at startup. Open a fresh connection per probe / write.
        Jedis writeJedis = RedisServer.initJedis('127.0.0.1', promotedPort)
        writeJedis.set('repl:after:failover', 'world')

        then:
        // The follower eventually catches up to the new master through Sentinel-issued SLAVEOF.
        waitUntil(60, 1_000L) {
            Jedis probe = null
            try {
                probe = RedisServer.initJedis('127.0.0.1', followerPort)
                def info = probe.info('replication')
                return info.contains('role:slave') &&
                        info.contains("master_port:${promotedPort}") &&
                        info.contains('master_link_status:up') &&
                        probe.get('repl:after:failover') == 'world'
            } catch (Exception ignored) {
                return false
            } finally {
                probe?.close()
            }
        }

        cleanup:
        writeJedis?.close()
        masterJedis?.close()
        replica1Jedis?.close()
        replica2Jedis?.close()
        sentinelJedis?.close()

        sentinels.each { it?.stop() }
        sentinelThreads.each { it?.join(5_000) }

        master?.stop()
        replica1?.stop()
        replica2?.stop()
        masterThread?.join(5_000)
        replica1Thread?.join(5_000)
        replica2Thread?.join(5_000)

        new File(baseDir).deleteDir()
    }

    private static VeloServer newVeloServer(String name, String dir) {
        new VeloServer(name)
                .randomPort()
                .dir(dir)
                .arg('sentinelModeEnabled', 'true')
                .arg('sentinelMasterName', MASTER_NAME)
                .arg('replicaAnnounceIp', '127.0.0.1')
                // Use a small hash-index capacity so the initial replica sync iterates far
                // fewer buckets and the wiring/failover windows stay tight.
                .arg('estimateKeyNumber', '1000000')
    }

    /**
     * Start N real redis-server --sentinel processes. Each sentinel gets its own port,
     * working dir, and config file under {@code ${baseDir}/sentinel-${i}/sentinel.conf}.
     * Sentinel stdout/stderr goes to {@code sentinel.log} in the same dir so it does not
     * fill the OS pipe buffer (RedisServer.run() does not drain stdout).
     *
     * @return [list of RedisServer, list of running Thread]
     */
    private static List startSentinels(String baseDir, String masterHost, int masterPort, int count) {
        def servers = []
        def threads = []
        for (int i = 0; i < count; i++) {
            def port = RedisServer.getOnePortListenAvailable()
            def dir = new File("${baseDir}/sentinel-${i}")
            dir.mkdirs()
            def confPath = new File(dir, "sentinel.conf").absolutePath

            def sb = new StringBuilder()
            sb << "port ${port}\n"
            sb << "bind 127.0.0.1\n"
            sb << "protected-mode no\n"
            sb << "daemonize no\n"
            sb << "dir ${dir.absolutePath}\n"
            sb << "logfile ${dir.absolutePath}/sentinel.log\n"
            sb << "sentinel monitor ${MASTER_NAME} ${masterHost} ${masterPort} 2\n"
            sb << "sentinel down-after-milliseconds ${MASTER_NAME} 3000\n"
            sb << "sentinel failover-timeout ${MASTER_NAME} 30000\n"
            sb << "sentinel parallel-syncs ${MASTER_NAME} 1\n"
            new File(confPath).text = sb.toString()

            def sentinel = new RedisServer("sentinel-${i}", confPath, '--sentinel')
            sentinel.port = port
            def thread = Thread.start("redis-sentinel-${i}") {
                sentinel.run()
            }
            servers << sentinel
            threads << thread
        }
        return [servers, threads]
    }

    /**
     * Poll the two Velo replica ports until exactly one of them reports {@code role:master}.
     * Sentinel's reported master address can flap during the election; this helper locks onto
     * the replica that has actually completed its {@code SLAVEOF NO ONE} transition, which is
     * the only thing the rest of the test cares about.
     *
     * @return the port of the promoted replica, or {@code -1} on timeout.
     */
    private static int waitForPromotedReplica(int replicaPort1, int replicaPort2, int retrySeconds) {
        for (int i = 0; i < retrySeconds; i++) {
            boolean r1Master = isMaster(replicaPort1)
            boolean r2Master = isMaster(replicaPort2)
            if (r1Master ^ r2Master) {
                return r1Master ? replicaPort1 : replicaPort2
            }
            Thread.sleep(1_000L)
        }
        return -1
    }

    private static boolean isMaster(int port) {
        Jedis probe = null
        try {
            probe = RedisServer.initJedis('127.0.0.1', port)
            return probe.info('replication').contains('role:master')
        } catch (Exception ignored) {
            return false
        } finally {
            probe?.close()
        }
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
