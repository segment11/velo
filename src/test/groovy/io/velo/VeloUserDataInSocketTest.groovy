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
        !one.isConnectionReadonly
        one.replyMode == VeloUserDataInSocket.ReplyMode.on
        one.lastScanAssignCursor == 0
        one.beginScanSeq == 0
        one.replPairAsSlaveInTcpClient == null
        two.replPairAsSlaveInTcpClient != null

        VeloUserDataInSocket.ReplyMode.from('on') == VeloUserDataInSocket.ReplyMode.on
        VeloUserDataInSocket.ReplyMode.from('off') == VeloUserDataInSocket.ReplyMode.off
        VeloUserDataInSocket.ReplyMode.from('skip') == VeloUserDataInSocket.ReplyMode.skip

        when:
        one.replyMode = VeloUserDataInSocket.ReplyMode.off
        one.lastScanAssignCursor = 1
        one.beginScanSeq = 1
        one.clientName = 'xxx'
        then:
        one.replyMode == VeloUserDataInSocket.ReplyMode.off
        one.lastScanAssignCursor == 1
        one.beginScanSeq == 1
        one.clientName == 'xxx'
    }
}
