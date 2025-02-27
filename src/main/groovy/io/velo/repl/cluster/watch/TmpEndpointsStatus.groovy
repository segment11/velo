package io.velo.repl.cluster.watch

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor

/**
 * Represents the status of endpoints in various clusters, specifically capturing
 * the slave listen address and the status of each individual endpoint associated
 * with each cluster.
 * For json serialization purposes.
 */
@CompileStatic
@TupleConstructor
class TmpEndpointsStatus {
    /**
     * The listen address of the slave node.
     * This address is used for communication purposes within the cluster.
     */
    String slaveListenAddress

    /**
     * A map where the key is the cluster name and the value is another map.
     * The inner map's key is an instance of {@link HostAndPort} representing
     * a specific host and port combination, and the value is the status of
     * the endpoint associated with that host and port, represented by an
     * instance of {@link OneEndpointStatus}.
     */
    Map<String, Map<HostAndPort, OneEndpointStatus>> oneEndpointStatusMapByClusterName = [:]
}