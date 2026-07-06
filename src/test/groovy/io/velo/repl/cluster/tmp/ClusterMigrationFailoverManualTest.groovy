package io.velo.repl.cluster.tmp

import io.velo.repl.cluster.ClusterxBase
import spock.lang.IgnoreIf

/**
 * Opt-in process-level cluster regression tests.
 *
 * Run only when explicitly requested:
 *   VELO_CLUSTER_MANUAL=true ./gradlew :test --tests "io.velo.repl.cluster.tmp.ClusterMigrationFailoverManualTest" --rerun-tasks
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
        waitForValue(nodes[3], key, 'before-migrate')
    }

    def 'manual repro: forced failover should preserve replicated data on promoted replica'() {
        given:
        startCluster()
        bootstrapInitialTopology()
        def key = findKeyForSlot(STEP)
        assert nodes[1].jedis.set(key, 'before-failover') == 'OK'
        waitForValue(nodes[3], key, 'before-failover')

        when:
        assert status(clusterx(nodes[3], 'failover', 'force')) == 'OK'
        broadcastSetNodes(failoverNodesArgs(false))

        then:
        clusterInfo(nodes[3]).contains('cluster_state:ok')
        nodes[3].jedis.get(key) == 'before-failover'
    }
}
