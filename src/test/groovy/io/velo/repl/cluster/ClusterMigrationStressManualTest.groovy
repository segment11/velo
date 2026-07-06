package io.velo.repl.cluster

import spock.lang.IgnoreIf

import java.util.concurrent.atomic.AtomicBoolean

/**
 * Opt-in process-level migration durability tests (data survives slot migration under stress).
 *
 * Run only when explicitly requested:
 *   VELO_CLUSTER_MANUAL=true ./gradlew :test --tests "io.velo.repl.cluster.ClusterMigrationStressManualTest" --rerun-tasks
 *
 * The temp base dir (per-node data and server logs) is deleted after each run; add
 * VELO_CLUSTER_KEEP_DIR=true to keep it for post-mortem when a scenario fails.
 */
@IgnoreIf({ System.getenv('VELO_CLUSTER_MANUAL') != 'true' && System.getProperty('velo.cluster.manual') != 'true' })
class ClusterMigrationStressManualTest extends ClusterxBase {

    def 'T1: big string values survive slot migration to the new owner and replica'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        def tag = hashTagForSlot(0)
        // 8 KiB values, above the 4096 big-string threshold configured for these nodes
        def entries = (0..<5).collect { ["{${tag}}:big:${it}".toString(), (bigValue(8) + '-' + it).toString()] }

        when: 'write big-string values on the source, then migrate the slot'
        entries.each { kv -> assert nodes[0].jedis.set(kv[0], kv[1]) == 'OK' }

        then: 'the values actually took the big-string path on the source'
        bigStringFileCount(0) > 0

        when:
        migrateSlot(0, nodes[0], nodes[1])

        then: 'the old owner redirects and every big-string value is intact on the new owner'
        movedFrom(nodes[0], entries[0][0])
        entries.every { kv -> nodes[1].jedis.get(kv[0]) == kv[1] }

        and: 'the new owner stored them as big strings and its replica received every value'
        bigStringFileCount(1) > 0
        entries.each { kv -> waitForValue(nodes[3], kv[0], kv[1]) }
    }

    def 'T2: concurrent writes during slot migration are not lost'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        def tag = hashTagForSlot(0)
        assert nodes[0].jedis.set("{${tag}}:seed".toString(), 'seed-value') == 'OK'

        def stop = new AtomicBoolean(false)
        def acked = Collections.synchronizedList([] as List<List<String>>)
        def writer = startBackgroundWriter(nodes[0], tag, stop, acked, 20)

        when: 'writes flow through the exists + catch-up window, then stop so the slave can catch up'
        Thread.sleep(500)
        // start the migration (import on target, export on source) with writes still in flight
        assert status(manage(nodes[1], 'slot', '0', 'migrate_from', HOST, nodes[0].port.toString(), 'force')) == 'OK'
        assert status(clusterx(nodes[0], 'migrate', '0', nodes[1].nodeId)) == 'OK'
        // keep writing across the exists/catch-up window, then quiesce so allCaughtUp is reachable
        Thread.sleep(1500)
        stop.set(true)
        writer.join(5000)
        waitForInfo(nodes[0], 'migrating_state:success')
        broadcastSetSlot(0, nodes[1].nodeId)

        then: 'writes were actually flowing, the old owner now redirects, seed survived'
        acked.size() > 0
        movedFrom(nodes[0], "{${tag}}:seed".toString())
        nodes[1].jedis.get("{${tag}}:seed".toString()) == 'seed-value'

        and: 'every write acked by the source before cutover is present on the new owner'
        acked.every { kv -> nodes[1].jedis.get(kv[0]) == kv[1] }

        and: 'the new owner replica also receives every acked write'
        acked.each { kv -> waitForValue(nodes[3], kv[0], kv[1]) }

        cleanup:
        stop?.set(true)
    }
}
