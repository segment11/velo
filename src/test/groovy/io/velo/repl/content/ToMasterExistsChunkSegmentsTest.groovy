package io.velo.repl.content

import io.activej.bytebuf.ByteBuf
import spock.lang.Specification

import java.nio.ByteBuffer

class ToMasterExistsChunkSegmentsTest extends Specification {
    def 'test all'() {
        given:
        def metaBytes = new byte[40]
        Arrays.fill(metaBytes, (byte) 1)

        def content = new ToMasterExistsChunkSegments(0, 10, metaBytes)

        expect:
        content.encodeLength() == 48

        when:
        def bytes = new byte[content.encodeLength()]
        def buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        def buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getInt() == 0
        buffer.getInt() == 10
        buffer.remaining() == 40

        when:
        def contentMetaBytes = new byte[48]
        then:
        !ToMasterExistsChunkSegments.isSlaveSameForThisBatch(metaBytes, contentMetaBytes)

        when:
        Arrays.fill(contentMetaBytes, (byte) 1)
        then:
        ToMasterExistsChunkSegments.isSlaveSameForThisBatch(metaBytes, contentMetaBytes)

        when:
        boolean exception = false
        contentMetaBytes = new byte[47]
        try {
            ToMasterExistsChunkSegments.isSlaveSameForThisBatch(metaBytes, contentMetaBytes)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
