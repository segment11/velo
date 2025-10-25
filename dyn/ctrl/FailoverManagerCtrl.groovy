import io.velo.ConfForGlobal
import io.velo.repl.LeaderSelector
import io.velo.repl.cluster.watch.FailoverManager
import io.velo.repl.cluster.watch.HostAndPort
import io.velo.repl.cluster.watch.TmpEndpointsStatus
import io.velo.repl.support.JedisPoolHolder
import org.segment.web.handler.ChainHandler
import org.slf4j.LoggerFactory

def h = ChainHandler.instance

def log = LoggerFactory.getLogger(this.getClass())

h.exceptionHandler { req, resp, t ->
    log.error('', t)
    resp.status = 500

    if (t instanceof AssertionError) {
        resp.outputStream << t.message
        return
    }

    log.error 'http request handle error', t
//    resp.outputStream << t.message ?: Utils.getStackTraceString(t)
    resp.outputStream << 'internal server error'
}

h.get('/failover_manager/leader') { req, resp ->
    def leaderSelector = LeaderSelector.instance
    [isMaster: leaderSelector.hasLeadership(), listenAddress: ConfForGlobal.netListenAddresses]
}

// one cluster meta manage
h.get('/failover_manager/view_all_endpoints_status') { req, resp ->
    FailoverManager.instance.oneEndpointStatusMapByClusterName
}

h.get('/failover_manager/view_endpoints_status_by_one_cluster_name') { req, resp ->
    def oneClusterName = req.param('oneClusterName')
    assert oneClusterName

    FailoverManager.instance.oneEndpointStatusMapByClusterName[oneClusterName]
}

h.get('/failover_manager/add_one_cluster_name') { req, resp ->
    def oneClusterName = req.param('oneClusterName')
    def masterHostAndPort = req.param('masterHostAndPort')
    assert oneClusterName && masterHostAndPort

    def array = masterHostAndPort.split(':')
    assert array.size() == 2
    def host = array[0]
    def port = array[1] as int

    def jedisPool = JedisPoolHolder.instance.createIfNotCached(host, port)
    def r = JedisPoolHolder.exe(jedisPool) { jedis ->
        jedis.clusterNodes()
    }

    List<HostAndPort> masterHostAndPortList = []

    def lines = r.readLines().collect { it.trim() }.findAll { it }
    lines.findAll { it.contains('master') }.each {
        def arr = it.split(' ')
        def masterHost = arr[1]
        def masterPort = arr[2] as int
        masterHostAndPortList << new HostAndPort(masterHost, masterPort)
    }

    FailoverManager.instance.addOneMetaToZookeeper(oneClusterName, masterHostAndPortList)
    [success: true]
}

h.get('/failover_manager/remove_one_cluster_name') { req, resp ->
    def oneClusterName = req.param('oneClusterName')
    assert oneClusterName

    FailoverManager.instance.removeOneMetaFromZookeeper(oneClusterName)
    [success: true]
}

// skip one cluster name manage
h.get('/failover_manager/view_skip_one_cluster_name_list') { req, resp ->
    [skipOneClusterNameSet: FailoverManager.instance.skipOneClusterNameSet]
}

h.get('/failover_manager/add_skip_one_cluster_name') { req, resp ->
    def oneClusterName = req.param('oneClusterName')
    assert oneClusterName

    FailoverManager.instance.addSkipOneClusterName(oneClusterName)
    [success: true]
}

h.get('/failover_manager/remove_skip_one_cluster_name') { req, resp ->
    def oneClusterName = req.param('oneClusterName')
    assert oneClusterName

    FailoverManager.instance.removeSkipOneClusterName(oneClusterName)
    [success: true]
}

// receive endpoints status slave post
h.post('/failover_manager/endpoints_status/slave_post') { req, resp ->
    def tmp = req.bodyAs(TmpEndpointsStatus)
    FailoverManager.instance.addOneEndpointStatusMapByClusterNamePostBySlave(tmp.slaveListenAddress, tmp.oneEndpointStatusMapByClusterName)
    [success: true]
}