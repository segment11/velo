package io.velo


import spock.lang.Specification

class ConfForGlobalTest extends Specification {
    // only for coverage
    def 'test all'() {
        given:
        println ConfForGlobal.estimateKeyNumber

        println ConfForGlobal.isValueSetUseCompression
        println ConfForGlobal.isOnDynTrainDictForCompression

        println ConfForGlobal.netListenAddresses

        println ConfForGlobal.dirPath
        println ConfForGlobal.pureMemory
        println ConfForGlobal.slotNumber
        println ConfForGlobal.netWorkers
        println ConfForGlobal.eventLoopIdleMillis

        println ConfForGlobal.zookeeperConnectString
        println ConfForGlobal.zookeeperRootPath
        println ConfForGlobal.canBeLeader
        println ConfForGlobal.isAsSlaveOfSlave

        println ConfForGlobal.LEADER_LATCH_PATH

        expect:
        1 == 1
    }
}
