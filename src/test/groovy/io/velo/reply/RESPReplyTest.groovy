package io.velo.reply

import io.activej.promise.SettablePromise
import spock.lang.Specification

class RESPReplyTest extends Specification {
    def 'test static'() {
        expect:
        OKReply.INSTANCE.buffer().asArray() == "+OK\r\n".bytes
        PongReply.INSTANCE.buffer().asArray() == "+PONG\r\n".bytes
        EmptyReply.INSTANCE.buffer().asArray().length == 0
        NilReply.INSTANCE.buffer().asArray() == "\$-1\r\n".bytes
        NilReply.INSTANCE.bufferAsResp3().asArray() == "_\r\n".bytes
        IntegerReply.REPLY_0.buffer().asArray() == ":0\r\n".bytes
        IntegerReply.REPLY_1.buffer().asArray() == ":1\r\n".bytes
        MultiBulkReply.EMPTY.buffer().asArray() == "*0\r\n".bytes
        MultiBulkReply.NULL.buffer().asArray() == "*-1\r\n".bytes
        MultiBulkReply.SCAN_EMPTY.replies.length == 2
        MultiBulkReply.SCAN_EMPTY.buffer().asArray() == "*2\r\n\$1\r\n0\r\n*0\r\n".bytes
        MultiBulkReply.EMPTY.dumpForTest(new StringBuilder(), 0)
        MultiBulkReply.SCAN_EMPTY.dumpForTest(new StringBuilder(), 1)
        ErrorReply.clusterMoved(100, 'localhost', 6380).message == 'MOVED 100 localhost:6380'
        ErrorReply.clusterMoved(100, 'localhost', 6380).buffer().asArray() == "-MOVED 100 localhost:6380\r\n".bytes
        Reply.DUMP_REPLY.buffer() == null
        Reply.DUMP_REPLY.bufferAsResp3() == null
        Reply.DUMP_REPLY.bufferAsHttp() == null
        Reply.DUMP_REPLY.dumpForTest(null, 0)
    }

    def 'test static as http'() {
        expect:
        OKReply.INSTANCE.bufferAsHttp().asArray() == 'OK'.bytes
        PongReply.INSTANCE.bufferAsHttp().asArray() == 'PONG'.bytes
        NilReply.INSTANCE.bufferAsHttp().asArray() == ''.bytes
        IntegerReply.REPLY_0.bufferAsHttp().asArray() == '0'.bytes
        IntegerReply.REPLY_1.bufferAsHttp().asArray() == '1'.bytes
        MultiBulkReply.EMPTY.bufferAsHttp().asArray() == '[]'.bytes
        MultiBulkReply.NULL.bufferAsHttp().asArray() == '[]'.bytes
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
        new DoubleReply(BigDecimal.valueOf(1.1)).buffer().asArray() == "\$3\r\n1.1\r\n".bytes
        new DoubleReply(BigDecimal.valueOf(1.1)).bufferAsResp3().asArray() == ",1.1\r\n".bytes
        new DoubleReply(BigDecimal.valueOf(1.1)).doubleValue() == 1.1d
        BoolReply.T.buffer().asArray() == "\$4\r\ntrue\r\n".bytes
        BoolReply.F.buffer().asArray() == "\$5\r\nfalse\r\n".bytes
        BoolReply.T.bufferAsResp3().asArray() == "#t\r\n".bytes
        BoolReply.F.bufferAsResp3().asArray() == "#f\r\n".bytes
        ErrorReply.WRONG_NUMBER('test').message == "wrong number of arguments for 'test' command"
        ErrorReply.WRONG_NUMBER('test').buffer().asArray() == "-ERR wrong number of arguments for 'test' command\r\n".bytes
        ErrorReply.NOT_INTEGER.message == 'value is not an integer or out of range'
        ErrorReply.NOT_INTEGER.buffer().asArray() == '-ERR value is not an integer or out of range\r\n'.bytes
        ErrorReply.INVALID_INTEGER.message == 'value is not an integer or out of range'
        ErrorReply.NOT_FLOAT.message == 'value is not a valid float'
        ErrorReply.NOT_FLOAT.buffer().asArray() == '-ERR value is not a valid float\r\n'.bytes
        ErrorReply.FORMAT.message == 'syntax error'
        ErrorReply.FORMAT.buffer().asArray() == '-ERR syntax error\r\n'.bytes
        ErrorReply.RANGE_OUT_OF_INDEX.message == 'value is out of range'
        ErrorReply.RANGE_OUT_OF_INDEX.buffer().asArray() == '-ERR value is out of range\r\n'.bytes
        ErrorReply.NOT_SUPPORT.message == 'operation not supported'
        ErrorReply.NOT_SUPPORT.buffer().asArray() == '-ERR operation not supported\r\n'.bytes
        ErrorReply.UNKNOWN_COMMAND('test').message == "unknown command 'test'"
        ErrorReply.UNKNOWN_COMMAND('test').buffer().asArray() == "-ERR unknown command 'test'\r\n".bytes
        new ErrorReply('error').message == 'error'
        new ErrorReply('error').buffer().asArray() == "-ERR error\r\n".bytes
        new ErrorReply('error').bufferAsHttp().asArray() == "error".bytes
        new ErrorReply('error').toString().contains('error')

        // redis aligned raw error code replies, texts refer to redis 7.2.14 source
        ErrorReply.WRONG_TYPE.message == 'WRONGTYPE Operation against a key holding the wrong kind of value'
        ErrorReply.WRONG_TYPE.buffer().asArray() == '-WRONGTYPE Operation against a key holding the wrong kind of value\r\n'.bytes
        ErrorReply.AUTH_FAILED.message == 'WRONGPASS invalid username-password pair or user is disabled.'
        ErrorReply.AUTH_FAILED.buffer().asArray() == '-WRONGPASS invalid username-password pair or user is disabled.\r\n'.bytes
        ErrorReply.NO_AUTH.message == 'NOAUTH Authentication required.'
        ErrorReply.NO_AUTH.buffer().asArray() == '-NOAUTH Authentication required.\r\n'.bytes
        ErrorReply.ACL_PERMIT_LIMIT.message == 'NOPERM this user has no permissions to run the command'
        ErrorReply.ACL_PERMIT_LIMIT.buffer().asArray() == '-NOPERM this user has no permissions to run the command\r\n'.bytes
        ErrorReply.ACL_PERMIT_KEY_LIMIT.message == 'NOPERM No permissions to access a key'
        ErrorReply.ACL_PERMIT_KEY_LIMIT.buffer().asArray() == '-NOPERM No permissions to access a key\r\n'.bytes
        ErrorReply.CLUSTER_SLOT_CROSS_SHARDS.message == "CROSSSLOT Keys in request don't hash to the same slot"
        ErrorReply.CLUSTER_SLOT_CROSS_SHARDS.buffer().asArray() == "-CROSSSLOT Keys in request don't hash to the same slot\r\n".bytes
        ErrorReply.CLUSTER_SLOT_NOT_SET.message == 'CLUSTERDOWN Hash slot not served'
        ErrorReply.CLUSTER_SLOT_NOT_SET.buffer().asArray() == '-CLUSTERDOWN Hash slot not served\r\n'.bytes
        ErrorReply.ACL_SETUSER_RULE_INVALID.message == 'Error in ACL SETUSER modifier'
        ErrorReply.ACL_SETUSER_RULE_INVALID.buffer().asArray() == '-ERR Error in ACL SETUSER modifier\r\n'.bytes

        BulkReply.numToBytes(100, true) == "100\r\n".bytes
        BulkReply.numToBytes(257, true) == "257\r\n".bytes
        BulkReply.numToBytes(-1, true) == "-1\r\n".bytes

        BulkReply.numToBytes(100, false) == "100".bytes
        BulkReply.numToBytes(257, false) == "257".bytes
        BulkReply.numToBytes(-1, false) == "-1".bytes

        new BulkReply('bulk').asString() == 'bulk'
        new BulkReply('bulk'.bytes).asString() == 'bulk'
        new BulkReply(1L).asString() == '1'
        new BulkReply(1.0d).asString() == '1.0'
        new BulkReply('bulk').buffer().asArray() == "\$4\r\nbulk\r\n".bytes
        new BulkReply('bulk').bufferAsResp3().asArray() == "+bulk\r\n".bytes
        new BulkReply('').bufferAsResp3().asArray() == "+\r\n".bytes
        new BulkReply((byte[]) null).bufferAsResp3().asArray() == "+\r\n".bytes
        // blob string as too long
        new BulkReply('12345678' * 5).bufferAsResp3().asArray()[0] == '$'.bytes[0]
        // blob string as has not printable char
        new BulkReply(('\r')).bufferAsResp3().asArray()[0] == '$'.bytes[0]
        new BulkReply('bulk').bufferAsHttp().asArray() == "bulk".bytes
        new BulkReply('bulk').dumpForTest(new StringBuilder(), 0)
        !new BulkReply(1L).equals(null)
        !new BulkReply(1L).equals(new Object())
        !new BulkReply(1L).equals(new BulkReply(2L))

        new MultiBulkReply(null).buffer().asArray() == "*-1\r\n".bytes
        new MultiBulkReply(null).bufferAsResp3().asArray() == "*-1\r\n".bytes
        Reply[] replies = [
                new BulkReply('bulk1'),
                BoolReply.T,
                new DoubleReply(1.0)
        ]
        new MultiBulkReply(replies).buffer().asArray() == '*3\r\n$5\r\nbulk1\r\n$4\r\ntrue\r\n$1\r\n1\r\n'.bytes
        new MultiBulkReply(replies).bufferAsResp3().asArray() == '*3\r\n+bulk1\r\n#t\r\n,1\r\n'.bytes
        new MultiBulkReply(replies).bufferAsResp3().asArray() == '*3\r\n+bulk1\r\n#t\r\n,1\r\n'.bytes
        new MultiBulkReply(replies).bufferAsHttp() != null

        when:
        def b0 = new BulkReply(0)
        def b00 = new BulkReply(0)
        then:
        b0.equals(b0)
        b0.equals(b00)

        when:
        Reply[] repliesMap = [
                new BulkReply('key1'),
                new BulkReply('value1'),
                new BulkReply('key2'),
                BoolReply.T
        ]
        then:
        new MultiBulkReply(repliesMap, true, false).bufferAsResp3().asArray() == '%2\r\n+key1\r\n+value1\r\n+key2\r\n#t\r\n'.bytes
        new MultiBulkReply(repliesMap, false, true).bufferAsResp3().asArray() == '~4\r\n+key1\r\n+value1\r\n+key2\r\n#t\r\n'.bytes
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
