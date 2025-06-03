package io.velo

import io.velo.repl.ReplPairTest
import spock.lang.Specification

class VeloUserDataInSocketTest extends Specification {
    def 'test base'() {
        given:
        def one = new VeloUserDataInSocket()
        def two = new VeloUserDataInSocket(ReplPairTest.mockAsSlave())

        expect:
        one.connectedTimeMillis > 0L
        one.lastSendCommandTimeMillis > 0L
        one.lastSendCommand == null
        one.sendCommandCount == 0L
        one.lastSetSeq == 0L
        one.lastSetSlot == (short) 0
        one.netInBytesLength == 0L
        one.netOutBytesLength == 0L
        one.authUser == null
        !one.isResp3
        one.libName == null
        one.libVer == null
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
        one.libName = 'Jedis'
        one.libVer = '4.3'
        then:
        one.replyMode == VeloUserDataInSocket.ReplyMode.off
        one.lastScanAssignCursor == 1
        one.beginScanSeq == 1
        one.clientName == 'xxx'
        one.libName == 'Jedis'
        one.libVer == '4.3'
    }
}
