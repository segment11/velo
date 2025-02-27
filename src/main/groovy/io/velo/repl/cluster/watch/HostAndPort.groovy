package io.velo.repl.cluster.watch

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.TupleConstructor

/**
 * Represents a network endpoint defined by a host and a port.
 * This class is used to encapsulate the details of a network location.
 */
@CompileStatic
@EqualsAndHashCode
@TupleConstructor
class HostAndPort {
    /**
     * The hostname or IP address of the network endpoint.
     */
    String host

    /**
     * The port number associated with the network endpoint.
     */
    int port

    /**
     * Returns a string representation of this network endpoint in the format "host:port".
     *
     * @return a string representation of the host and port
     */
    @Override
    String toString() {
        "$host:$port"
    }
}