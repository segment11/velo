package io.velo.repl.content

import io.activej.bytebuf.ByteBuf
import spock.lang.Specification

class NextStepContentTest extends Specification {
    def 'test all'() {
        given:
        def content = NextStepContent.INSTANCE

        expect:
        content.encodeLength() == 1

        when:
        def bytes = new byte[1]
        def buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        then:
        bytes[0] == 0
        NextStepContent.isNextStep(bytes)
    }
}
