package io.velo.repl.cluster.watch

import groovy.transform.CompileStatic
import groovy.transform.EqualsAndHashCode
import groovy.transform.TupleConstructor

@CompileStatic
@EqualsAndHashCode
@TupleConstructor
class HostAndPort {
    String host
    int port

    @Override
    String toString() {
        "$host:$port"
    }
}
