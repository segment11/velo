package io.velo.repl.cluster.watch

import com.github.kevinsawicki.http.HttpRequest
import com.segment.common.Conf
import com.segment.common.Utils
import io.prometheus.client.exporter.HTTPServer
import io.prometheus.client.hotspot.DefaultExports
import io.velo.ConfForGlobal
import io.velo.repl.LeaderSelector
import io.velo.repl.support.JedisPoolHolder
import org.segment.web.RouteRefreshLoader
import org.segment.web.RouteServer
import org.segment.web.common.CachedGroovyClassLoader
import org.segment.web.handler.ChainHandler
import org.slf4j.LoggerFactory

import java.util.concurrent.Executors
import java.util.concurrent.TimeUnit

def log = LoggerFactory.getLogger(this.getClass())

// project work directory set
String[] x = super.binding.getProperty('args') as String[]
def c = Conf.instance.resetWorkDir().load().loadArgs(x)
log.info c.toString()

def projectDir = new File(c.projectPath('/'))
projectDir.eachFile {
    if (it.name.endsWith('.jar') && it.name.startsWith('velo')) {
        c.on('server.runtime.jar')
        log.info 'running in jar'
    }
}

ConfForGlobal.jedisPoolMaxTotal = c.getInt('jedisPoolMaxTotal', 2)
ConfForGlobal.jedisPoolMaxIdle = c.getInt('jedisPoolMaxIdle', 1)
ConfForGlobal.jedisPoolMaxWaitMillis = c.getInt('jedisPoolMaxWaitMillis', 5000)

HttpRequest.keepAlive(true)

def listenPort = c.getInt('listenPort', 27379)
def listenHost = c.getString('listenHost', '0.0.0.0')

if (!Utils.isPortListenAvailable(listenPort, listenHost)) {
    log.info 'velo guarder server is already running'
    return
}
ConfForGlobal.netListenAddresses = listenHost + ':' + listenPort

def zookeeperConnectString = c.get('zookeeperConnectString')
if (zookeeperConnectString == null) {
    log.error 'config zookeeperConnectString is null'
    return
}
def zookeeperListenHost = zookeeperConnectString.split(':')[0]
def zookeeperListenPort = zookeeperConnectString.split(':')[1] as int

if (Utils.isPortListenAvailable(zookeeperListenPort, zookeeperListenHost)) {
    log.error 'zookeeper is not available'
    return
}
ConfForGlobal.zookeeperConnectString = zookeeperConnectString

def zookeeperVeloMetaBasePath = c.getString('zookeeperVeloFailoverManagerBasePath', '/velo/failover_manager')
FailoverManager.instance.zookeeperVeloMetaBasePath = zookeeperVeloMetaBasePath
ConfForGlobal.zookeeperRootPath = zookeeperVeloMetaBasePath

def schedulePeriodSeconds = c.getInt('schedulePeriodSeconds', 5)

def singleThreadScheduledExecutor = Executors.newSingleThreadScheduledExecutor()
singleThreadScheduledExecutor.scheduleAtFixedRate(() -> {
    LeaderSelector.instance.tryConnectAndGetMasterListenAddress(true)
    try {
        FailoverManager.instance.checkFailover()
    } catch (Exception e) {
        log.error 'failover manager check failover error', e
    }
}, schedulePeriodSeconds, schedulePeriodSeconds, TimeUnit.SECONDS)
log.info 'leader selector and failover manager scheduled'

// chain filter uri prefix set
ChainHandler.instance.context('/velo')

def srcDirPath = c.projectPath('/dyn/ctrl')

// groovy class loader init
def loader = CachedGroovyClassLoader.instance
loader.init(c.class.classLoader, srcDirPath)

def server = RouteServer.instance
server.loader = RouteRefreshLoader.create(loader.gcl).addClasspath(srcDirPath).
        addDir(c.projectPath('/dyn/ctrl')).jarLoad(c.isOn('server.runtime.jar'))

server.start(listenPort, listenHost)
log.info 'velo guarder server started - {}', listenPort

// prometheus metrics
DefaultExports.initialize()
def metricsServer = new HTTPServer.Builder().
        withPort(listenPort + 10000).withHostname(listenHost).withDaemonThreads(true).build()
log.info 'velo guarder prometheus metrics server started - {}', listenPort + 10000

def stopCl = {
    singleThreadScheduledExecutor.shutdown()
    log.info 'leader selector and failover manager scheduled stopped'
    JedisPoolHolder.instance.cleanUp()
    log.info 'jedis pool holder clean up'
    metricsServer.close()
    log.info 'velo guarder prometheus metrics server stopped'
    server.stop()
    log.info 'velo guarder server stopped'
}
Runtime.addShutdownHook stopCl
