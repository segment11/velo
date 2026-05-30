package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.CompressedValue;

/**
 * Error reply with server error messages.
 */
public class ErrorReply implements Reply {

    /** Error reply for format error. */
    public static final ErrorReply FORMAT = new ErrorReply("format");

    /**
     * @param cmd the command name
     * @return error reply for wrong number of arguments
     */
    public static ErrorReply WRONG_NUMBER(String cmd) {
        return new ErrorReply("*wrong number of arguments for '" + cmd + "' command");
    }

    /**
     * @param cmd the unknown command
     * @return error reply for unknown command
     */
    public static ErrorReply UNKNOWN_COMMAND(String cmd) {
        return new ErrorReply("unknown command '" + cmd + "'");
    }

    /** Error reply for missing dictionary. */
    public static final ErrorReply DICT_MISSING = new ErrorReply("dict missing");

    /** Error reply for key too long. */
    public static final ErrorReply KEY_TOO_LONG = new ErrorReply("key too long (max length is " + CompressedValue.KEY_MAX_LENGTH + ")");

    /** Error reply for value too long. */
    public static final ErrorReply VALUE_TOO_LONG = new ErrorReply("value too long (max length is " + CompressedValue.VALUE_MAX_LENGTH + ")");

    /** Error reply for server stopped. */
    public static final ErrorReply SERVER_STOPPED = new ErrorReply("server stopped");

    /** Error reply for authentication failure. */
    public static final ErrorReply AUTH_FAILED = new ErrorReply("auth failed !WRONGPASS!");

    /** Error reply for missing authentication. */
    public static final ErrorReply NO_AUTH = new ErrorReply("no auth");

    /** Error reply for ACL permit limit reached. */
    public static final ErrorReply ACL_PERMIT_LIMIT = new ErrorReply("user acl permit limit !NOPERM!");

    /** Error reply for ACL permit limit reached for a key. */
    public static final ErrorReply ACL_PERMIT_KEY_LIMIT = new ErrorReply("user acl permit limit !NOPERM! !key!");

    /** Error reply for invalid ACL SETUSER modifier. */
    public static final ErrorReply ACL_SETUSER_RULE_INVALID = new ErrorReply("!Error in ACL SETUSER modifier!");

    /** Error reply for bloom filter already exists. */
    public static final ErrorReply BF_ALREADY_EXISTS = new ErrorReply("bloom filter already exists");

    /** Error reply for bloom filter not exists. */
    public static final ErrorReply BF_NOT_EXISTS = new ErrorReply("bloom filter not exists");

    /** Error reply for syntax error. */
    public static final ErrorReply SYNTAX = new ErrorReply("syntax error");

    /** Error reply for not an integer. */
    public static final ErrorReply NOT_INTEGER = new ErrorReply("not integer");

    /** Error reply for not a float. */
    public static final ErrorReply NOT_FLOAT = new ErrorReply("not float");

    /** Error reply for not a string. */
    public static final ErrorReply NOT_STRING = new ErrorReply("not string");

    /** Error reply for invalid integer. */
    public static final ErrorReply INVALID_INTEGER = new ErrorReply("invalid integer");

    /** Error reply for invalid file. */
    public static final ErrorReply INVALID_FILE = new ErrorReply("invalid file");

    /** Error reply for range out of index. */
    public static final ErrorReply RANGE_OUT_OF_INDEX = new ErrorReply("range out of index");

    /** Error reply for key does not exist. */
    public static final ErrorReply NO_SUCH_KEY = new ErrorReply("not such key");

    /** Error reply for wrong type. */
    public static final ErrorReply WRONG_TYPE = new ErrorReply("wrong type");

    /** Error reply for list size too long. */
    public static final ErrorReply LIST_SIZE_TO_LONG = new ErrorReply("list size too long");

    /** Error reply for hash size too long. */
    public static final ErrorReply HASH_SIZE_TO_LONG = new ErrorReply("hash size too long");

    /** Error reply for set size too long. */
    public static final ErrorReply SET_SIZE_TO_LONG = new ErrorReply("set size too long");

    /** Error reply for set member length too long. */
    public static final ErrorReply SET_MEMBER_LENGTH_TO_LONG = new ErrorReply("set member length too long");

    /** Error reply for zset size too long. */
    public static final ErrorReply ZSET_SIZE_TO_LONG = new ErrorReply("zset size too long");

    /** Error reply for zset member length too long. */
    public static final ErrorReply ZSET_MEMBER_LENGTH_TO_LONG = new ErrorReply("zset member length too long");

    /** Error reply for index out of range. */
    public static final ErrorReply INDEX_OUT_OF_RANGE = new ErrorReply("index out of range");

    /** Error reply for value that must be positive. */
    public static final ErrorReply VALUE_NOT_POSITIVE = new ErrorReply("value is not an integer or out of range");

    /** Error reply for read-only mode. */
    public static final ErrorReply READONLY = new ErrorReply("readonly");

    /** Error reply for cannot read. */
    public static final ErrorReply CANNOT_READ = new ErrorReply("cannot read");

    /** Error reply for unsupported operation. */
    public static final ErrorReply NOT_SUPPORT = new ErrorReply("not support");

    /** Error reply for unsupported dump type. */
    public static final ErrorReply DUMP_TYPE_NOT_SUPPORT = new ErrorReply("dump type not support");

    /** Error reply for cluster slot crossing shards. */
    public static final ErrorReply CLUSTER_SLOT_CROSS_SHARDS = new ErrorReply("cluster slot cross shards");

    /** Error reply for cluster slot not set. */
    public static final ErrorReply CLUSTER_SLOT_NOT_SET = new ErrorReply("cluster slot not set");

    /** Error reply for file does not exist. */
    public static final ErrorReply NO_SUCH_FILE = new ErrorReply("no such file");

    /** Error reply for target key busy. */
    public static final ErrorReply TARGET_KEY_BUSY = new ErrorReply("target key name is busy");

    /**
     * @param toClientSlot the target slot index
     * @param host         the target host
     * @param port         the target port
     * @return MOVED error reply
     */
    public static ErrorReply clusterMoved(int toClientSlot, String host, int port) {
        return new ErrorReply("MOVED " + toClientSlot + " " + host + ":" + port, true);
    }

    private final String message;
    private final boolean rawErrorPrefix;

    /**
     * @param message the error message
     */
    public ErrorReply(String message) {
        this(message, false);
    }

    private ErrorReply(String message, boolean rawErrorPrefix) {
        this.message = message;
        this.rawErrorPrefix = rawErrorPrefix;
    }

    public String getMessage() {
        return message;
    }

    @Override
    public String toString() {
        return "ErrorReply{" +
                "message='" + message + '\'' +
                '}';
    }

    @Override
    public ByteBuf buffer() {
        var bytes = ((rawErrorPrefix ? "-" : "-ERR ") + message + "\r\n").getBytes();
        return ByteBuf.wrapForReading(bytes);
    }

    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(message.getBytes());
    }
}
