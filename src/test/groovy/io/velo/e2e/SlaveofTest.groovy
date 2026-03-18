package io.velo.e2e

import io.velo.test.tools.VeloServer
import spock.lang.Specification

class SlaveOfTest extends Specification {
    def 'test slaveof'() {
        given:
        if (!VeloServer.isJarExists()) {
            println 'velo jar not built, skip'
            expect:
            true
            return
        }

        def suffix = System.nanoTime()
        def master = new VeloServer('slaveof-master')
                .randomPort()
                .dir("/tmp/velo-e2e-master-${suffix}")
        def slave = new VeloServer('slaveof-slave')
                .randomPort()
                .dir("/tmp/velo-e2e-slave-${suffix}")

        def masterThread = Thread.start {
            master.run()
        }
        def slaveThread = Thread.start {
            slave.run()
        }

        and:
        def masterJedis = master.jedis()
        def slaveJedis = slave.jedis()
        masterJedis.set('repl:test:before', 'hello')

        when:
        def slaveofResult = slaveJedis.slaveof('127.0.0.1', master.port)

        then:
        slaveofResult == 'OK'
        waitUntil(60, 1_000L) {
            try {
                def replicationInfo = slaveJedis.info('replication')
                return replicationInfo.contains('role:slave') &&
                        replicationInfo.contains('master_host:127.0.0.1') &&
                        replicationInfo.contains("master_port:${master.port}") &&
                        replicationInfo.contains('master_link_status:up') &&
                        slaveJedis.get('repl:test:before') == 'hello'
            } catch (Exception ignored) {
                return false
            }
        }

        when:
        masterJedis.set('repl:test:after', 'world')

        then:
        waitUntil(20, 1_000L) {
            try {
                slaveJedis.get('repl:test:after') == 'world'
            } catch (Exception ignored) {
                false
            }
        }

        cleanup:
        masterJedis?.close()
        slaveJedis?.close()
        slave.stop()
        master.stop()
        slaveThread?.join(5_000)
        masterThread?.join(5_000)
        new File("/tmp/velo-e2e-master-${suffix}").deleteDir()
        new File("/tmp/velo-e2e-slave-${suffix}").deleteDir()
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
