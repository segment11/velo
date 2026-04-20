package io.velo.e2e

import io.velo.BaseCommand
import io.velo.test.tools.VeloServer
import spock.lang.Specification

class SlaveOfTest extends Specification {
    def 'test slaveof'() {
        given:
        def suffix = System.nanoTime()
        VeloServer master = null
        VeloServer slave = null
        Thread masterThread = null
        Thread slaveThread = null
        def masterJedis = null
        def slaveJedis = null
        if (!VeloServer.isJarExists()) {
            println 'velo jar not built, skip'
            expect:
            true
            return
        }

        master = new VeloServer('slaveof-master')
                .randomPort()
                .dir("/tmp/velo-e2e-master-${suffix}")
        slave = new VeloServer('slaveof-slave')
                .randomPort()
                .dir("/tmp/velo-e2e-slave-${suffix}")

        masterThread = Thread.start {
            master.run()
        }
        slaveThread = Thread.start {
            slave.run()
        }

        and:
        masterJedis = master.jedis()
        slaveJedis = slave.jedis()
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
        slave?.stop()
        master?.stop()
        slaveThread?.join(5_000)
        masterThread?.join(5_000)
        new File("/tmp/velo-e2e-master-${suffix}").deleteDir()
        new File("/tmp/velo-e2e-slave-${suffix}").deleteDir()
    }

    def 'test slaveof when slave has more slots than master'() {
        given:
        def suffix = System.nanoTime()
        VeloServer master = null
        VeloServer slave = null
        Thread masterThread = null
        Thread slaveThread = null
        def masterJedis = null
        def slaveJedis = null
        if (!VeloServer.isJarExists()) {
            println 'velo jar not built, skip'
            expect:
            true
            return
        }

        master = new VeloServer('slaveof-master-slot-1')
                .randomPort()
                .dir("/tmp/velo-e2e-master-slot-1-${suffix}")
        slave = new VeloServer('slaveof-slave-slot-2')
                .randomPort()
                .dir("/tmp/velo-e2e-slave-slot-2-${suffix}")
                .arg('slotNumber', 2)

        masterThread = Thread.start {
            master.run()
        }
        slaveThread = Thread.start {
            slave.run()
        }

        and:
        masterJedis = master.jedis()
        slaveJedis = slave.jedis()
        def keyBefore = findKeyForSlot('repl:test:before:slot0', 0, 2)
        def keyAfter = findKeyForSlot('repl:test:after:slot0', 0, 2)
        masterJedis.set(keyBefore, 'hello')

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
                        slaveJedis.get(keyBefore) == 'hello'
            } catch (Exception ignored) {
                return false
            }
        }

        when:
        masterJedis.set(keyAfter, 'world')

        then:
        waitUntil(20, 1_000L) {
            try {
                slaveJedis.get(keyAfter) == 'world'
            } catch (Exception ignored) {
                false
            }
        }

        cleanup:
        masterJedis?.close()
        slaveJedis?.close()
        slave?.stop()
        master?.stop()
        slaveThread?.join(5_000)
        masterThread?.join(5_000)
        new File("/tmp/velo-e2e-master-slot-1-${suffix}").deleteDir()
        new File("/tmp/velo-e2e-slave-slot-2-${suffix}").deleteDir()
    }

    def 'test slaveof with slave extra slot does not expose keys hashed to slave only slot'() {
        given:
        def suffix = System.nanoTime()
        VeloServer master = null
        VeloServer slave = null
        Thread masterThread = null
        Thread slaveThread = null
        def masterJedis = null
        def slaveJedis = null
        if (!VeloServer.isJarExists()) {
            println 'velo jar not built, skip'
            expect:
            true
            return
        }

        master = new VeloServer('slaveof-master-slot-1-bug')
                .randomPort()
                .dir("/tmp/velo-e2e-master-slot-1-bug-${suffix}")
        slave = new VeloServer('slaveof-slave-slot-2-bug')
                .randomPort()
                .dir("/tmp/velo-e2e-slave-slot-2-bug-${suffix}")
                .arg('slotNumber', 2)

        masterThread = Thread.start {
            master.run()
        }
        slaveThread = Thread.start {
            slave.run()
        }

        and:
        masterJedis = master.jedis()
        slaveJedis = slave.jedis()
        def slot0Key = findKeyForSlot('repl:test:slot0:visible', 0, 2)
        def slot1Key = findKeyForSlot('repl:test:slot1:hidden', 1, 2)
        masterJedis.set(slot0Key, 'visible')
        masterJedis.set(slot1Key, 'hidden')

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
                        slaveJedis.get(slot0Key) == 'visible'
            } catch (Exception ignored) {
                return false
            }
        }

        when:
        def slot1Result = readValueOrError(slaveJedis, slot1Key)
        println "Slave read for slave-only slot key: ${slot1Result}"

        then:
        slot1Result != 'hidden'

        cleanup:
        masterJedis?.close()
        slaveJedis?.close()
        slave?.stop()
        master?.stop()
        slaveThread?.join(5_000)
        masterThread?.join(5_000)
        new File("/tmp/velo-e2e-master-slot-1-bug-${suffix}").deleteDir()
        new File("/tmp/velo-e2e-slave-slot-2-bug-${suffix}").deleteDir()
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

    private static String findKeyForSlot(String prefix, int expectedSlot, int slotNumber) {
        for (int i = 0; i < 10_000; i++) {
            def key = "${prefix}:${i}"
            if (BaseCommand.slot(key, slotNumber).slot() == (short) expectedSlot) {
                return key
            }
        }
        throw new IllegalStateException("No key found for slot=${expectedSlot}, slotNumber=${slotNumber}")
    }

    private static String readValueOrError(def jedis, String key) {
        try {
            return jedis.get(key)
        } catch (Exception e) {
            return "ERR:${e.class.simpleName}:${e.message}"
        }
    }
}
