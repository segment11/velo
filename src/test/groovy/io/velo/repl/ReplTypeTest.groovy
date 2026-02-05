package io.velo.repl

import spock.lang.Specification

class ReplTypeTest extends Specification {
    def 'test all'() {
        given:
//        final short slot = 0

        def types = ReplType.values()

        expect:
        types.length == 24

        ReplType.fromCode(ReplType.ping.code) == ReplType.ping
        ReplType.fromCode((byte) -10) == null

        ReplType.ping.newly
        ReplType.ping.isSlaveSend
        ReplType.ping.code == (byte) -1
    }
}
