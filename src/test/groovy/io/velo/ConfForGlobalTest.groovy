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
        println ConfForGlobal.isPureMemoryModeKeyBucketsUseCompression

        println ConfForGlobal.netListenAddresses

        println ConfForGlobal.dirPath
        println ConfForGlobal.pureMemory
        println ConfForGlobal.pureMemoryV2
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
}
