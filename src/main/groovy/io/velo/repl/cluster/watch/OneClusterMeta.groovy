package io.velo.repl.cluster.watch

import groovy.transform.CompileStatic

/**
 * Represents metadata for a single cluster, including a list of master hosts and their corresponding ports.
 * For json serialization purposes.
 */
@CompileStatic
class OneClusterMeta {
    /**
     * A list containing the host and port information for each master node in the cluster.
     */
    List<HostAndPort> masterHostAndPortList
}