package io.velo

import io.velo.repl.ReplPairTest
import spock.lang.Specification

class VeloUserDataInSocketTest extends Specification {
    def 'test base'() {
        given:
        def one = new VeloUserDataInSocket()
        def two = new VeloUserDataInSocket(ReplPairTest.mockAsSlave())

        expect:
        !one.isResp3
        one.authUser == null
        one.lastScanTargetKeyBytes == null
        one.lastScanAssignCursor == 0
        one.replPairAsSlaveInTcpClient == null
        two.replPairAsSlaveInTcpClient != null

        when:
        one.lastScanTargetKeyBytes = 'key'.bytes
        one.lastScanAssignCursor = 1
        one.clientName = 'xxx'
        then:
        one.lastScanTargetKeyBytes == 'key'.bytes
        one.lastScanAssignCursor == 1
        one.clientName == 'xxx'
    }
}
