package io.velo

import io.velo.acl.AclUsers
import spock.lang.Specification

class ConfForGlobalTest extends Specification {
    // only for coverage
    def 'test all'() {
        given:
        println ConfForGlobal.datacenterId
        println ConfForGlobal.machineId
        println ConfForGlobal.estimateKeyNumber
        println ConfForGlobal.estimateOneValueLength
        println ConfForGlobal.keyAnalysisNumberPercent

        println ConfForGlobal.isValueSetUseCompression
        println ConfForGlobal.isOnDynTrainDictForCompression

        println ConfForGlobal.netListenAddress

        println ConfForGlobal.dirPath
        println ConfForGlobal.slotNumber
        println ConfForGlobal.slotWorkers
        println ConfForGlobal.netWorkers
        println ConfForGlobal.eventloopIdleMillis

        println ConfForGlobal.zookeeperConnectString
        println ConfForGlobal.zookeeperSessionTimeoutMs
        println ConfForGlobal.zookeeperConnectionTimeoutMs
        println ConfForGlobal.zookeeperRootPath
        println ConfForGlobal.canBeLeader
        println ConfForGlobal.isAsSlaveOfSlave

        println ConfForGlobal.LEADER_LATCH_PATH

        println ConfForGlobal.doubleScale
        println ConfForGlobal.initDynConfigItems

        expect:
        1 == 1

        when:
        boolean exception = false
        ConfForGlobal.PASSWORD = '123456'
        AclUsers.instance.initForTest()
        try {
            ConfForGlobal.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        !exception

        when:
        exception = false
        ConfForGlobal.estimateKeyNumber = 10_000_000L * 2
        try {
            ConfForGlobal.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        ConfForGlobal.estimateKeyNumber = 10_000_000L
        ConfForGlobal.keyAnalysisNumberPercent = 0
        try {
            ConfForGlobal.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        ConfForGlobal.keyAnalysisNumberPercent = 101
        try {
            ConfForGlobal.checkIfValid()
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test sentinel config defaults'() {
        expect:
        // Defaults documented in the plan / ConfForGlobal javadoc.
        ConfForGlobal.sentinelModeEnabled == false
        ConfForGlobal.sentinelMasterName == 'mymaster'
        ConfForGlobal.replicaAnnounceIp == null
        ConfForGlobal.replicaAnnouncePort == 0
        ConfForGlobal.sentinelReplicaPriority == 100
    }

    def 'test checkIfValid for sentinel config'() {
        given:
        // 'test all' may have left keyAnalysisNumberPercent = 101 dirty; reset for valid baseline.
        def savedPercent = ConfForGlobal.keyAnalysisNumberPercent
        def savedSentinelEnabled = ConfForGlobal.sentinelModeEnabled
        def savedZk = ConfForGlobal.zookeeperConnectString
        def savedPriority = ConfForGlobal.sentinelReplicaPriority
        ConfForGlobal.keyAnalysisNumberPercent = 1
        ConfForGlobal.sentinelModeEnabled = false
        ConfForGlobal.zookeeperConnectString = null

        boolean exception = false
        String message = null

        when: 'negative sentinel replica priority is rejected'
        try {
            ConfForGlobal.sentinelReplicaPriority = -1
            ConfForGlobal.checkIfValid()
        } catch (IllegalArgumentException e) {
            message = e.message
            exception = true
        }
        then:
        exception
        message.contains('Sentinel replica priority')

        when: 'sentinel mode combined with zookeeper is rejected'
        exception = false
        message = null
        ConfForGlobal.sentinelReplicaPriority = 100
        ConfForGlobal.sentinelModeEnabled = true
        ConfForGlobal.zookeeperConnectString = '127.0.0.1:2181'
        try {
            ConfForGlobal.checkIfValid()
        } catch (IllegalArgumentException e) {
            message = e.message
            exception = true
        }
        then:
        exception
        message.contains('Sentinel mode and ZooKeeper mode')

        when: 'sentinel mode without zookeeper is accepted'
        exception = false
        message = null
        ConfForGlobal.zookeeperConnectString = null
        try {
            ConfForGlobal.checkIfValid()
        } catch (IllegalArgumentException e) {
            message = e.message
            exception = true
        }
        then:
        !exception

        cleanup:
        ConfForGlobal.keyAnalysisNumberPercent = savedPercent
        ConfForGlobal.sentinelModeEnabled = savedSentinelEnabled
        ConfForGlobal.zookeeperConnectString = savedZk
        ConfForGlobal.sentinelReplicaPriority = savedPriority
    }

    def 'test announcedHostPort'() {
        given:
        def savedListen = ConfForGlobal.netListenAddress
        def savedIp = ConfForGlobal.replicaAnnounceIp
        def savedPort = ConfForGlobal.replicaAnnouncePort

        when: 'no announce configured and listen address is host:port'
        ConfForGlobal.netListenAddress = '127.0.0.1:7379'
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        def result = ConfForGlobal.announcedHostPort()
        then:
        result.host == '127.0.0.1'
        result.port == 7379

        when: 'listen address with another IPv4 port'
        ConfForGlobal.netListenAddress = '0.0.0.0:6380'
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == '0.0.0.0'
        result.port == 6380

        when: 'listen address with hostname'
        ConfForGlobal.netListenAddress = 'redis-master.local:6381'
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == 'redis-master.local'
        result.port == 6381

        when: 'only announce IP is set — port falls back to listen port'
        ConfForGlobal.netListenAddress = '127.0.0.1:7379'
        ConfForGlobal.replicaAnnounceIp = '10.0.0.5'
        ConfForGlobal.replicaAnnouncePort = 0
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == '10.0.0.5'
        result.port == 7379

        when: 'only announce port is set — host falls back to listen host'
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 9999
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == '127.0.0.1'
        result.port == 9999

        when: 'both announce IP and port are set'
        ConfForGlobal.replicaAnnounceIp = '192.168.1.10'
        ConfForGlobal.replicaAnnouncePort = 16379
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == '192.168.1.10'
        result.port == 16379

        when: 'empty announce IP is treated as unset'
        ConfForGlobal.netListenAddress = 'redis-host:6390'
        ConfForGlobal.replicaAnnounceIp = ''
        ConfForGlobal.replicaAnnouncePort = 0
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == 'redis-host'
        result.port == 6390

        when: 'listen address has no colon — host only, port stays 0'
        ConfForGlobal.netListenAddress = 'myhost'
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == 'myhost'
        result.port == 0

        when: 'listen address has unparseable port after colon — port falls through to 0'
        ConfForGlobal.netListenAddress = 'myhost:notanumber'
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == 'myhost'
        result.port == 0

        when: 'listen address ends with trailing colon'
        ConfForGlobal.netListenAddress = 'myhost:'
        result = ConfForGlobal.announcedHostPort()
        then: 'idx == length - 1, host/port are not parsed from the string'
        // current behavior keeps the whole string as host, port stays 0
        result.host == 'myhost:'
        result.port == 0

        when: 'netListenAddress is null and announce is unset'
        ConfForGlobal.netListenAddress = null
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        result = ConfForGlobal.announcedHostPort()
        then:
        result.host == null
        result.port == 0

        cleanup:
        ConfForGlobal.netListenAddress = savedListen
        ConfForGlobal.replicaAnnounceIp = savedIp
        ConfForGlobal.replicaAnnouncePort = savedPort
    }

    def 'test announcedHostPortString'() {
        given:
        def savedListen = ConfForGlobal.netListenAddress
        def savedIp = ConfForGlobal.replicaAnnounceIp
        def savedPort = ConfForGlobal.replicaAnnouncePort

        when: 'no announce configured — string mirrors the listen address'
        ConfForGlobal.netListenAddress = '127.0.0.1:7379'
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        then:
        ConfForGlobal.announcedHostPortString() == '127.0.0.1:7379'

        when: 'only announce IP is set — port falls back to listen port'
        ConfForGlobal.netListenAddress = '0.0.0.0:7379'
        ConfForGlobal.replicaAnnounceIp = '10.0.0.5'
        ConfForGlobal.replicaAnnouncePort = 0
        then:
        ConfForGlobal.announcedHostPortString() == '10.0.0.5:7379'

        when: 'both announce IP and port are set'
        ConfForGlobal.replicaAnnounceIp = '10.0.0.5'
        ConfForGlobal.replicaAnnouncePort = 7380
        then:
        ConfForGlobal.announcedHostPortString() == '10.0.0.5:7380'

        when: 'empty announce IP is treated as unset — falls back to listen host'
        ConfForGlobal.netListenAddress = '0.0.0.0:7379'
        ConfForGlobal.replicaAnnounceIp = ''
        ConfForGlobal.replicaAnnouncePort = 0
        then:
        ConfForGlobal.announcedHostPortString() == '0.0.0.0:7379'

        when: 'listen address is null and announce is unset — returns raw null'
        ConfForGlobal.netListenAddress = null
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        then:
        ConfForGlobal.announcedHostPortString() == null

        when: 'listen address is the empty string — host parses to empty, falls back to raw'
        ConfForGlobal.netListenAddress = ''
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        then:
        ConfForGlobal.announcedHostPortString() == ''

        cleanup:
        ConfForGlobal.netListenAddress = savedListen
        ConfForGlobal.replicaAnnounceIp = savedIp
        ConfForGlobal.replicaAnnouncePort = savedPort
    }
}
