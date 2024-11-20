package io.velo.reply

import io.activej.promise.SettablePromise
import spock.lang.Specification

class RESPReplyTest extends Specification {
    def 'test static'() {
        expect:
        OKReply.INSTANCE.buffer().asArray() == "+OK\r\n".bytes
        PongReply.INSTANCE.buffer().asArray() == "+PONG\r\n".bytes
        NilReply.INSTANCE.buffer().asArray() == "\$-1\r\n".bytes
        IntegerReply.REPLY_0.buffer().asArray() == ":0\r\n".bytes
        IntegerReply.REPLY_1.buffer().asArray() == ":1\r\n".bytes
        MultiBulkReply.EMPTY.buffer().asArray() == "*0\r\n".bytes
        MultiBulkReply.SCAN_EMPTY.replies.length == 2
        MultiBulkReply.SCAN_EMPTY.buffer().asArray() == "*2\r\n\$1\r\n0\r\n*0\r\n".bytes
        MultiBulkReply.EMPTY.dumpForTest(new StringBuilder(), 0)
        MultiBulkReply.SCAN_EMPTY.dumpForTest(new StringBuilder(), 1)
        ErrorReply.clusterMoved(100, 'localhost', 6380).message == 'MOVED 100 localhost:6380'
    }

    def 'test static as http'() {
        expect:
        OKReply.INSTANCE.bufferAsHttp().asArray() == 'OK'.bytes
        PongReply.INSTANCE.bufferAsHttp().asArray() == 'PONG'.bytes
        NilReply.INSTANCE.bufferAsHttp().asArray() == ''.bytes
        IntegerReply.REPLY_0.bufferAsHttp().asArray() == '0'.bytes
        IntegerReply.REPLY_1.bufferAsHttp().asArray() == '1'.bytes
        MultiBulkReply.EMPTY.bufferAsHttp().asArray() == '[]'.bytes
        NilReply.INSTANCE.dumpForTest(new StringBuilder(), 0)
        IntegerReply.REPLY_0.dumpForTest(new StringBuilder(), 0)
        IntegerReply.REPLY_1.dumpForTest(new StringBuilder(), 0)
        new IntegerReply(100).dumpForTest(new StringBuilder(), 0)
    }

    def 'test others'() {
        expect:
        IntegerReply.bufferPreload(null).asArray() == "\$-1\r\n".bytes
        new IntegerReply(100).integer == 100
        new IntegerReply(100).buffer().asArray() == ":100\r\n".bytes
        new IntegerReply(100).bufferAsHttp().asArray() == "100".bytes
        new DoubleReply(new BigDecimal(1.1)).buffer().asArray() == "\$4\r\n1.10\r\n".bytes
        new DoubleReply(new BigDecimal(1.1)).bufferAsResp3().asArray() == ",1.10\r\n".bytes
        new DoubleReply(new BigDecimal(1.1)).doubleValue() == 1.1d
        BoolReply.T.buffer().asArray() == "\$4\r\ntrue\r\n".bytes
        BoolReply.F.buffer().asArray() == "\$5\r\nfalse\r\n".bytes
        BoolReply.T.bufferAsResp3().asArray() == "#t\r\n".bytes
        BoolReply.F.bufferAsResp3().asArray() == "#f\r\n".bytes
        new ErrorReply('error').message == 'error'
        new ErrorReply('error').buffer().asArray() == "-ERR error\r\n".bytes
        new ErrorReply('error').bufferAsHttp().asArray() == "error".bytes
        new ErrorReply('error').toString().contains('error')

        BulkReply.numToBytes(100, true) == "100\r\n".bytes
        BulkReply.numToBytes(257, true) == "257\r\n".bytes
        BulkReply.numToBytes(-1, true) == "-1\r\n".bytes

        BulkReply.numToBytes(100, false) == "100".bytes
        BulkReply.numToBytes(257, false) == "257".bytes
        BulkReply.numToBytes(-1, false) == "-1".bytes

        new BulkReply('bulk'.bytes).raw == 'bulk'.bytes
        new BulkReply('bulk'.bytes).buffer().asArray() == "\$4\r\nbulk\r\n".bytes
        new BulkReply('bulk'.bytes).bufferAsHttp().asArray() == "bulk".bytes
        new BulkReply('bulk'.bytes).dumpForTest(new StringBuilder(), 0)

        new MultiBulkReply(null).buffer().asArray() == "*-1\r\n".bytes
        Reply[] replies = [
                new BulkReply('bulk1'.bytes),
                new BulkReply('bulk2'.bytes)
        ]
        new MultiBulkReply(replies).buffer().asArray() == "*2\r\n\$5\r\nbulk1\r\n\$5\r\nbulk2\r\n".bytes
        new MultiBulkReply(replies).bufferAsHttp() != null
    }

    def 'test async reply'() {
        // just a wrapper, no need to test
        given:
        SettablePromise<Reply> finalPromise = new SettablePromise<>()
        def asyncReply = new AsyncReply(finalPromise)

        expect:
        asyncReply.settablePromise == finalPromise
        asyncReply.buffer() == null
    }
}
