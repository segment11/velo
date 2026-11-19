package io.velo.reply

import spock.lang.Specification

class NullArrayReplyTest extends Specification {
    def 'test null array reply'() {
        expect:
        // redis answers a blocking command that timed out with a null array in RESP2
        NullArrayReply.INSTANCE.buffer().asArray() == '*-1\r\n'.getBytes()
        // and with the null type in RESP3
        NullArrayReply.INSTANCE.bufferAsResp3().asArray() == '_\r\n'.getBytes()
        new String(NullArrayReply.INSTANCE.bufferAsHttp().asArray()) == '[]'
        // it is not the null bulk of NilReply
        NullArrayReply.INSTANCE.buffer().asArray() != NilReply.INSTANCE.buffer().asArray()
    }
}
