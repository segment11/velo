package io.velo.e2e

import io.velo.BaseCommand
import io.velo.test.tools.VeloServer
import spock.lang.Specification

class SlaveofTest extends Specification {
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

        // ---- batch incremental catch-up: write many keys, check all land on slave ----
        when:
        def batchCount = 500
        def batchKeys = []
        for (int i = 0; i < batchCount; i++) {
            def k = "repl:test:batch:${i}:${suffix}"
            batchKeys << k
            masterJedis.set(k, "value-${i}")
        }

        then:
        waitUntil(60, 1_000L) {
            int matched = 0
            for (k in batchKeys) {
                try {
                    if (slaveJedis.get(k) == "value-${batchKeys.indexOf(k)}") {
                        matched++
                    }
                } catch (Exception ignored) {
                }
            }
            if (matched != batchCount) {
                println "batch catch-up progress: ${matched}/${batchCount}"
            }
            matched == batchCount
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

    def 'test slaveof 2N scale-up fans out keys to slave extra slot'() {
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

        master = new VeloServer('slaveof-master-slot-1-fanout')
                .randomPort()
                .dir("/tmp/velo-e2e-master-slot-1-fanout-${suffix}")
        slave = new VeloServer('slaveof-slave-slot-2-fanout')
                .randomPort()
                .dir("/tmp/velo-e2e-slave-slot-2-fanout-${suffix}")
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
        def slot0Key = findKeyForSlot('repl:test:slot0:stream', 0, 2)
        def slot1Key = findKeyForSlot('repl:test:slot1:fanout', 1, 2)
        masterJedis.set(slot0Key, 'visible')
        masterJedis.set(slot1Key, 'fanned-out')

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

        // In 2N scale-up mode (slave slotNumber == master slotNumber * 2), the single
        // master stream slot fans out to every slave slot: each key is re-hashed under
        // the slave's slotNumber (see XWalV/XBigStrings.targetSlot) and written to
        // whichever slot it lands in. So a key hashing to the slave extra slot [N, 2N)
        // IS expected to be exposed (doc/design/09_replication_design.md § 2N Scale-Up).
        when:
        def slot1Result = readValueOrError(slaveJedis, slot1Key)
        println "Slave read for fanned-out extra-slot key: ${slot1Result}"

        then:
        slot1Result == 'fanned-out'

        cleanup:
        masterJedis?.close()
        slaveJedis?.close()
        slave?.stop()
        master?.stop()
        slaveThread?.join(5_000)
        masterThread?.join(5_000)
        new File("/tmp/velo-e2e-master-slot-1-fanout-${suffix}").deleteDir()
        new File("/tmp/velo-e2e-slave-slot-2-fanout-${suffix}").deleteDir()
    }

    def 'test slaveof master N=2 slave 2N=4'() {
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

        // master has 2 slots, slave has 4 (= 2 × master) → exercises 2N scale-up with
        // TWO stream slots (0,1) and TWO extra slots (2,3). The global read gate must
        // wait for BOTH streams to be ready before opening.
        master = new VeloServer('slaveof-master-slot-2')
                .randomPort()
                .dir("/tmp/velo-e2e-master-slot-2-${suffix}")
                .arg('slotNumber', 2)
        slave = new VeloServer('slaveof-slave-slot-4')
                .randomPort()
                .dir("/tmp/velo-e2e-slave-slot-4-${suffix}")
                .arg('slotNumber', 4)

        masterThread = Thread.start {
            master.run()
        }
        slaveThread = Thread.start {
            slave.run()
        }

        and:
        masterJedis = master.jedis()
        slaveJedis = slave.jedis()

        // pick keys that land in specific slots on the slave (slotNumber=4), including
        // extra slots [2,4) whose data arrives only via fan-out from stream slots [0,2).
        def keySlot0 = findKeyForSlot('repl:test:n2:slot0', 0, 4)
        def keySlot1 = findKeyForSlot('repl:test:n2:slot1', 1, 4)
        def keySlot2 = findKeyForSlot('repl:test:n2:slot2', 2, 4)
        def keySlot3 = findKeyForSlot('repl:test:n2:slot3', 3, 4)
        masterJedis.set(keySlot0, 'val0')
        masterJedis.set(keySlot1, 'val1')
        masterJedis.set(keySlot2, 'val2')
        masterJedis.set(keySlot3, 'val3')

        when:
        def slaveofResult = slaveJedis.slaveof('127.0.0.1', master.port)

        then:
        slaveofResult == 'OK'
        // initial sync: all four keys (including extra-slot keys 2,3) must be readable
        waitUntil(60, 1_000L) {
            try {
                def replicationInfo = slaveJedis.info('replication')
                return replicationInfo.contains('role:slave') &&
                        replicationInfo.contains('master_host:127.0.0.1') &&
                        replicationInfo.contains("master_port:${master.port}") &&
                        replicationInfo.contains('master_link_status:up') &&
                        slaveJedis.get(keySlot0) == 'val0' &&
                        slaveJedis.get(keySlot1) == 'val1' &&
                        slaveJedis.get(keySlot2) == 'val2' &&
                        slaveJedis.get(keySlot3) == 'val3'
            } catch (Exception ignored) {
                return false
            }
        }

        // ---- incremental: single key per slave target slot ----
        when:
        def incKeySlot0 = findKeyForSlot('repl:test:n2:inc:slot0', 0, 4)
        def incKeySlot3 = findKeyForSlot('repl:test:n2:inc:slot3', 3, 4)
        masterJedis.set(incKeySlot0, 'inc-val0')
        masterJedis.set(incKeySlot3, 'inc-val3')

        then:
        waitUntil(30, 1_000L) {
            try {
                slaveJedis.get(incKeySlot0) == 'inc-val0' &&
                        slaveJedis.get(incKeySlot3) == 'inc-val3'
            } catch (Exception ignored) {
                false
            }
        }

        // ---- batch incremental catch-up: write many keys, check all land on slave ----
        when:
        def batchCount = 500
        def batchKeys = []
        for (int i = 0; i < batchCount; i++) {
            def k = "repl:test:n2:batch:${i}:${suffix}"
            batchKeys << k
            masterJedis.set(k, "value-${i}")
        }

        then:
        waitUntil(60, 1_000L) {
            int matched = 0
            for (int i = 0; i < batchKeys.size(); i++) {
                try {
                    if (slaveJedis.get(batchKeys[i]) == "value-${i}") {
                        matched++
                    }
                } catch (Exception ignored) {
                }
            }
            if (matched != batchCount) {
                println "N=2 batch catch-up progress: ${matched}/${batchCount}"
            }
            matched == batchCount
        }

        cleanup:
        masterJedis?.close()
        slaveJedis?.close()
        slave?.stop()
        master?.stop()
        slaveThread?.join(5_000)
        masterThread?.join(5_000)
        new File("/tmp/velo-e2e-master-slot-2-${suffix}").deleteDir()
        new File("/tmp/velo-e2e-slave-slot-4-${suffix}").deleteDir()
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
