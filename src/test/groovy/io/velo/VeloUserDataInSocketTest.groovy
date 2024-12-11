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
        one.replPairAsSlaveInTcpClient == null
        two.replPairAsSlaveInTcpClient != null
    }
}
