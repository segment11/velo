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

        println ConfForGlobal.netListenAddresses

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

    def 'test getAnnouncedHostAndPort'() {
        given:
        def savedListen = ConfForGlobal.netListenAddresses
        def savedIp = ConfForGlobal.replicaAnnounceIp
        def savedPort = ConfForGlobal.replicaAnnouncePort

        when: 'no announce configured and listen address is host:port'
        ConfForGlobal.netListenAddresses = '127.0.0.1:7379'
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        def result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result.length == 2
        result[0] == '127.0.0.1'
        result[1] == '7379'

        when: 'listen address with another IPv4 port'
        ConfForGlobal.netListenAddresses = '0.0.0.0:6380'
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result[0] == '0.0.0.0'
        result[1] == '6380'

        when: 'listen address with hostname'
        ConfForGlobal.netListenAddresses = 'redis-master.local:6381'
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result[0] == 'redis-master.local'
        result[1] == '6381'

        when: 'only announce IP is set — port falls back to listen port'
        ConfForGlobal.netListenAddresses = '127.0.0.1:7379'
        ConfForGlobal.replicaAnnounceIp = '10.0.0.5'
        ConfForGlobal.replicaAnnouncePort = 0
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result[0] == '10.0.0.5'
        result[1] == '7379'

        when: 'only announce port is set — host falls back to listen host'
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 9999
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result[0] == '127.0.0.1'
        result[1] == '9999'

        when: 'both announce IP and port are set'
        ConfForGlobal.replicaAnnounceIp = '192.168.1.10'
        ConfForGlobal.replicaAnnouncePort = 16379
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result[0] == '192.168.1.10'
        result[1] == '16379'

        when: 'empty announce IP is treated as unset'
        ConfForGlobal.netListenAddresses = 'redis-host:6390'
        ConfForGlobal.replicaAnnounceIp = ''
        ConfForGlobal.replicaAnnouncePort = 0
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result[0] == 'redis-host'
        result[1] == '6390'

        when: 'listen address has no colon — host only, port stays 0'
        ConfForGlobal.netListenAddresses = 'myhost'
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result[0] == 'myhost'
        result[1] == '0'

        when: 'listen address has unparseable port after colon — port falls through to 0'
        ConfForGlobal.netListenAddresses = 'myhost:notanumber'
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result[0] == 'myhost'
        result[1] == '0'

        when: 'listen address ends with trailing colon'
        ConfForGlobal.netListenAddresses = 'myhost:'
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then: 'idx == length - 1, host/port are not parsed from the string'
        // current behavior keeps the whole string as host, port stays 0
        result[0] == 'myhost:'
        result[1] == '0'

        when: 'netListenAddresses is null and announce is unset'
        ConfForGlobal.netListenAddresses = null
        ConfForGlobal.replicaAnnounceIp = null
        ConfForGlobal.replicaAnnouncePort = 0
        result = ConfForGlobal.getAnnouncedHostAndPort()
        then:
        result.length == 2
        result[0] == null
        result[1] == '0'

        cleanup:
        ConfForGlobal.netListenAddresses = savedListen
        ConfForGlobal.replicaAnnounceIp = savedIp
        ConfForGlobal.replicaAnnouncePort = savedPort
    }
}
