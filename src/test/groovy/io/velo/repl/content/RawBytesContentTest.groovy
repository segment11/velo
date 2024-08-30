package io.velo.repl.content

import io.activej.bytebuf.ByteBuf
import spock.lang.Specification

class RawBytesContentTest extends Specification {
    def 'test all'() {
        given:
        def rawBytes = 'xxx'.bytes
        def content = new RawBytesContent(rawBytes)

        expect:
        content.encodeLength() == rawBytes.length

        when:
        def bytes = new byte[content.encodeLength()]
        def buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        then:
        bytes == rawBytes

        when:
        boolean exception = false
        try {
            new RawBytesContent(null)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            new RawBytesContent(new byte[0])
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
