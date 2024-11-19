package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.CompressedValue;

public class ErrorReply implements Reply {
    public static final ErrorReply FORMAT = new ErrorReply("format");
    public static final ErrorReply DICT_MISSING = new ErrorReply("dict missing");
    public static final ErrorReply KEY_TOO_LONG = new ErrorReply("key too long (max length is " + CompressedValue.KEY_MAX_LENGTH + ")");
    public static final ErrorReply VALUE_TOO_LONG = new ErrorReply("value too long (max length is " + CompressedValue.VALUE_MAX_LENGTH + ")");
    public static final ErrorReply SERVER_STOPPED = new ErrorReply("server stopped");
    public static final ErrorReply AUTH_FAILED = new ErrorReply("auth failed");
    public static final ErrorReply NO_AUTH = new ErrorReply("no auth");
    public static final ErrorReply ACL_PERMIT_LIMIT = new ErrorReply("user acl permit limit");
    public static final ErrorReply SYNTAX = new ErrorReply("syntax error");
    public static final ErrorReply NOT_INTEGER = new ErrorReply("not integer");
    public static final ErrorReply NOT_FLOAT = new ErrorReply("not float");
    public static final ErrorReply NOT_STRING = new ErrorReply("not string");
    public static final ErrorReply INVALID_INTEGER = new ErrorReply("invalid integer");
    public static final ErrorReply RANGE_OUT_OF_INDEX = new ErrorReply("range out of index");
    public static final ErrorReply NO_SUCH_KEY = new ErrorReply("not such key");
    public static final ErrorReply WRONG_TYPE = new ErrorReply("wrong type");
    public static final ErrorReply LIST_SIZE_TO_LONG = new ErrorReply("list size too long");
    public static final ErrorReply HASH_SIZE_TO_LONG = new ErrorReply("hash size too long");
    public static final ErrorReply SET_SIZE_TO_LONG = new ErrorReply("set size too long");
    public static final ErrorReply SET_MEMBER_LENGTH_TO_LONG = new ErrorReply("set member length too long");
    public static final ErrorReply ZSET_SIZE_TO_LONG = new ErrorReply("zset size too long");
    public static final ErrorReply ZSET_MEMBER_LENGTH_TO_LONG = new ErrorReply("zset member length too long");
    public static final ErrorReply INDEX_OUT_OF_RANGE = new ErrorReply("index out of range");
    public static final ErrorReply READONLY = new ErrorReply("readonly");
    public static final ErrorReply NOT_SUPPORT = new ErrorReply("not support");
    public static final ErrorReply CLUSTER_SLOT_CROSS_SHARDS = new ErrorReply("cluster slot cross shards");
    public static final ErrorReply CLUSTER_SLOT_NOT_SET = new ErrorReply("cluster slot not set");
    public static final ErrorReply NO_SUCH_FILE = new ErrorReply("no such file");

    public static ErrorReply clusterMoved(int toClientSlot, String host, int port) {
        return new ErrorReply("MOVED " + toClientSlot + " " + host + ":" + port);
    }

    private final String message;

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "ErrorReply{" +
                "message='" + message + '\'' +
                '}';
    }

    public ErrorReply(String message) {
        this.message = message;
    }

    @Override
    public ByteBuf buffer() {
        var bytes = ("-ERR " + message + "\r\n").getBytes();
        return ByteBuf.wrapForReading(bytes);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(message.getBytes());
    }
}
