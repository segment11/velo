package io.velo.repl.cluster

import spock.lang.IgnoreIf

/**
 * Opt-in process-level cluster regression tests.
 *
 * Run only when explicitly requested:
 *   VELO_CLUSTER_MANUAL=true ./gradlew :test --tests "io.velo.repl.cluster.ClusterMigrationFailoverManualTest" --rerun-tasks
 *
 * The temp base dir (per-node data and server logs) is deleted after each run; add
 * VELO_CLUSTER_KEEP_DIR=true to keep it for post-mortem when a scenario fails.
 */
@IgnoreIf({ System.getenv('VELO_CLUSTER_MANUAL') != 'true' && System.getProperty('velo.cluster.manual') != 'true' })
class ClusterMigrationFailoverManualTest extends ClusterxBase {
    def 'manual repro: slot migration should preserve data on the new owner and target shard replica'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        def key = findKeyForSlot(0)
        assert nodes[0].jedis.set(key, 'before-migrate') == 'OK'
        assert nodes[0].jedis.get(key) == 'before-migrate'

        when:
        assert status(manage(nodes[1], 'slot', '0', 'migrate_from', HOST, nodes[0].port.toString(), 'force')) == 'OK'
        assert status(clusterx(nodes[0], 'migrate', '0', nodes[1].nodeId)) == 'OK'
        waitForInfo(nodes[0], 'migrating_state:success')
        broadcastSetSlot(0, nodes[1].nodeId)

        then:
        movedFrom(nodes[0], key)
        nodes[1].jedis.get(key) == 'before-migrate'
        // node3 is node1's replica; velo routes by shard (MultiWorkerServer.checkMoved), so a replica
        // in the owning shard serves reads locally without READONLY. A move to master-only reads would
        // turn this into a MOVED and time out here.
        waitForValue(nodes[3], key, 'before-migrate')
    }

    def 'manual repro: forced failover should preserve replicated data on promoted replica'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        def key = findKeyForSlot(STEP)
        assert nodes[1].jedis.set(key, 'before-failover') == 'OK'
        // node3 is node1's replica and serves shard-local reads (see the migration test's note).
        waitForValue(nodes[3], key, 'before-failover')

        when:
        // failover promotes node3; broadcastSetNodes then publishes the new topology at the harness's
        // own monotonic clusterVersion. This assumes 'failover force' does not itself bump the shard
        // version past it, otherwise the SETNODES would be rejected as stale and time out.
        assert status(clusterx(nodes[3], 'failover', 'force')) == 'OK'
        broadcastSetNodes(failoverNodesArgs(false))

        then:
        clusterInfo(nodes[3]).contains('cluster_state:ok')
        nodes[3].jedis.get(key) == 'before-failover'
    }
}
