package io.velo.repl.cluster.watch

import groovy.transform.CompileStatic
import groovy.transform.TupleConstructor

@CompileStatic
@TupleConstructor
// for json
class TmpEndpointsStatus {
    String slaveListenAddress
    Map<String, Map<HostAndPort, OneEndpointStatus>> oneEndpointStatusMapByClusterName = [:]
}
