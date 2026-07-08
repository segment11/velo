package io.velo.repl.cluster

import redis.clients.jedis.exceptions.JedisDataException
import spock.lang.IgnoreIf

import java.util.concurrent.TimeUnit
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

        when: 'migrate slot 0 to node-1, resyncing its replica node-3 before cutover (required now the gate'
        // enforces a synced replica before promotion)
        migrateSlot(0, nodes[0], nodes[1], [nodes[3]])

        then: 'the old owner redirects and every big-string value is intact on the new owner'
        movedFrom(nodes[0], entries[0][0])
        entries.every { kv -> nodes[1].jedis.get(kv[0]) == kv[1] }

        and: 'the new owner stored them as big strings and its replica received every value'
        bigStringFileCount(1) > 0
        entries.each { kv -> waitForValue(nodes[3], kv[0], kv[1]) }
    }

    def 'T3: big string values survive new-owner crash right after cutover (durability window closed)'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        def tag = hashTagForSlot(0)
        def entries = (0..<5).collect { ["{${tag}}:big:${it}".toString(), (bigValue(8) + '-' + it).toString()] }

        when: 'write big-string values on the source, then migrate with pre-cutover replica resync'
        entries.each { kv -> assert nodes[0].jedis.set(kv[0], kv[1]) == 'OK' }

        then: 'the values actually took the big-string path on the source'
        bigStringFileCount(0) > 0

        when: 'migrate slot 0 to node-1, resyncing its replica node-3 before cutover'
        migrateSlot(0, nodes[0], nodes[1], [nodes[3]])

        then: 'the new owner replica already holds every value at cutover (synced before, not after)'
        entries.each { kv -> waitForValue(nodes[3], kv[0], kv[1]) }

        when: 'the new owner crashes right after cutover, then its replica is promoted among living nodes'
        processes[1].destroyForcibly()
        processes[1].waitFor(5, TimeUnit.SECONDS)
        clusterVersion++
        def promoted = failoverNodesArgs(true)
        [nodes[0], nodes[2], nodes[3]].each { node ->
            waitClusterxOk(node, 'setnodes', promoted, clusterVersion.toString())
        }

        then: 'every big-string value survived on the promoted node — there was no no-replica window'
        entries.each { kv -> waitForValue(nodes[3], kv[0], kv[1]) }
    }

    def 'T4: migrating a non-zero inner slot releases it on the source, keeps its other slots, and can migrate back'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        // inner slot 2 = client 8192..12287 and inner slot 3 = client 12288..16383 are both owned by node-2
        def tag2 = hashTagForSlot(8192)
        def tag3 = hashTagForSlot(12288)
        def k2 = "{${tag2}}:k".toString()
        def k3 = "{${tag3}}:k".toString()

        when: 'write into inner slot 2 and inner slot 3 (both on node-2)'
        assert nodes[2].jedis.set(k2, 'val-slot2') == 'OK'
        assert nodes[2].jedis.set(k3, 'val-slot3') == 'OK'

        then:
        nodes[2].jedis.get(k2) == 'val-slot2'
        nodes[2].jedis.get(k3) == 'val-slot3'

        when: 'migrate inner slot 2 from node-2 to node-0 (node-0 has no replica, so no resync needed)'
        migrateSlot(8192, nodes[2], nodes[0])

        then: 'the value moved to node-0; node-2 redirects inner slot 2 but still serves inner slot 3'
        waitForValue(nodes[0], k2, 'val-slot2')
        movedFrom(nodes[2], k2)
        nodes[2].jedis.get(k3) == 'val-slot3'

        and: 'node-2 actually released the shed inner slot 2'
        waitServerLogContains(2, 'Released shed one slot=2')

        when: 'migrate inner slot 2 back from node-0 to node-2 (re-initializes the released slot)'
        migrateSlot(8192, nodes[0], nodes[2])

        then: 'the value is back on node-2 and node-0 redirects'
        waitForValue(nodes[2], k2, 'val-slot2')
        movedFrom(nodes[0], k2)
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
        // resync node-1's replica node-3 before cutover (the gate now requires a synced replica), then
        // broadcast charge-node-first, source-last
        assert status(manage(nodes[3], 'slot', '0', 'migrate_from', HOST, nodes[1].port.toString(), 'force')) == 'OK'
        broadcastSetSlotChargeFirst(0, nodes[1], nodes[0])

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

    def 'T5: aborting an in-flight import before cutover leaves the source owning the slot and a later migrate still works'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        // inner slot 2 = client 8192.. is owned by node-2; import it onto node-0 then abort before any cutover
        def tag = hashTagForSlot(8192)
        def k = "{${tag}}:k".toString()

        when: 'seed the slot on its owner, then start importing it onto node-0 (no ownership cutover yet)'
        assert nodes[2].jedis.set(k, 'val-before-abort') == 'OK'
        assert status(manage(nodes[0], 'slot', '8192', 'migrate_from', HOST, nodes[2].port.toString(), 'force')) == 'OK'
        assert status(clusterx(nodes[2], 'migrate', '8192', nodes[0].nodeId)) == 'OK'
        waitForInfo(nodes[2], 'migrating_state:success')

        and: 'abort the import on the target and cancel the export on the source, before any setslot'
        assert status(manage(nodes[0], 'slot', '8192', 'migrate_from_abort')) == 'OK'
        assert status(clusterx(nodes[2], 'migrate', '8192', 'cancel')) == 'OK'

        then: 'ownership never moved: the source still serves the slot and node-0 does not claim it'
        nodes[2].jedis.get(k) == 'val-before-abort'
        movedFrom(nodes[0], k)

        and: 'the source resumes accepting writes — cancel must clear the readonly the export set (bug 64)'
        // during export the source went readonly once the slave caught up; after cancel it still owns the
        // slot, so it must be writable again rather than stuck returning ERR readonly
        nodes[2].jedis.set(k, 'val-after-abort') == 'OK'
        nodes[2].jedis.get(k) == 'val-after-abort'
    }

    def 'T7: migrating a slot from a node that does not own it is rejected and info stays healthy'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        // node-0 owns inner slot 0 only; inner slot 2 (client 8192) belongs to node-2
        def foreignClientSlot = '8192'

        when: 'ask node-0 to export a slot it does not own'
        def err = null
        try {
            clusterx(nodes[0], 'migrate', foreignClientSlot, nodes[1].nodeId)
        } catch (JedisDataException e) {
            err = e.message
        }

        then: 'it is rejected as not owned by this shard'
        err != null
        err.contains('not owned by this shard')

        and: 'a following clusterx info on that node returns cleanly with no NPE despite the lazy/unowned slot'
        def info = clusterInfo(nodes[0])
        info.contains('cluster_state:ok')
        !info.contains('migrating_slot:8192')
    }
}
