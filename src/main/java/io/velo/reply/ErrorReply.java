package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.CompressedValue;

import java.util.Objects;

/**
 * Error reply with server error messages.
 */
public class ErrorReply implements Reply {

    /** Error reply for bad arguments structure, redis' syntax error text, used for both arity and bad-value as a simplification. */
    public static final ErrorReply FORMAT = new ErrorReply("syntax error");

    /**
     * @param cmd the command name
     * @return error reply for wrong number of arguments
     */
    public static ErrorReply WRONG_NUMBER(String cmd) {
        return new ErrorReply("wrong number of arguments for '" + cmd + "' command");
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

    /** Error reply for the migrate command with a not empty key argument and the keys option. */
    public static final ErrorReply MIGRATE_KEY_NOT_EMPTY = new ErrorReply(
            "When using MIGRATE KEYS option, the key argument must be set to the empty string");

    /** Error reply for value too long. */
    public static final ErrorReply VALUE_TOO_LONG = new ErrorReply("value too long (max length is " + CompressedValue.VALUE_MAX_LENGTH + ")");

    /** Error reply for server stopped. */
    public static final ErrorReply SERVER_STOPPED = new ErrorReply("server stopped");

    /** Error reply for authentication failure, the reply is WRONGPASS prefixed. */
    public static final ErrorReply AUTH_FAILED = new ErrorReply(
            "WRONGPASS invalid username-password pair or user is disabled.", true);

    /** Error reply for missing authentication, the reply is NOAUTH prefixed. */
    public static final ErrorReply NO_AUTH = new ErrorReply("NOAUTH Authentication required.", true);

    /** Error reply for ACL permit limit reached, the reply is NOPERM prefixed. */
    public static final ErrorReply ACL_PERMIT_LIMIT = new ErrorReply(
            "NOPERM this user has no permissions to run the command", true);

    /** Error reply for ACL permit limit reached for a key, the reply is NOPERM prefixed. */
    public static final ErrorReply ACL_PERMIT_KEY_LIMIT = new ErrorReply(
            "NOPERM No permissions to access a key", true);

    /** Error reply for invalid ACL SETUSER modifier. */
    public static final ErrorReply ACL_SETUSER_RULE_INVALID = new ErrorReply("Error in ACL SETUSER modifier");

    /** Error reply for bloom filter already exists. */
    public static final ErrorReply BF_ALREADY_EXISTS = new ErrorReply("bloom filter already exists");

    /** Error reply for bloom filter not exists. */
    public static final ErrorReply BF_NOT_EXISTS = new ErrorReply("bloom filter not exists");

    /** Error reply for syntax error. */
    public static final ErrorReply SYNTAX = new ErrorReply("syntax error");

    /** Error reply for not an integer, the text is aligned with redis. */
    public static final ErrorReply INVALID_INTEGER = new ErrorReply("value is not an integer or out of range");

    /** Error reply for not a float, the text is aligned with redis. */
    public static final ErrorReply INVALID_FLOAT = new ErrorReply("value is not a valid float");

    /** Error reply for a SORT element that cannot be converted into a double, the text is aligned with redis. */
    public static final ErrorReply SORT_SCORE_NOT_DOUBLE = new ErrorReply(
            "One or more scores can't be converted into double");

    /** Error reply for a NaN or Infinity value. */
    public static final ErrorReply NAN_OR_INFINITY = new ErrorReply("value is NaN or Infinity");

    /** Error reply for invalid file. */
    public static final ErrorReply INVALID_FILE = new ErrorReply("invalid file");

    /** Error reply for range out of index, the text is aligned with redis. */
    public static final ErrorReply RANGE_OUT_OF_INDEX = new ErrorReply("value is out of range");

    /** Error reply for key does not exist. */
    public static final ErrorReply NO_SUCH_KEY = new ErrorReply("no such key");

    /** Error reply for wrong type, the reply is WRONGTYPE prefixed, the text is aligned with redis. */
    public static final ErrorReply WRONG_TYPE = new ErrorReply(
            "WRONGTYPE Operation against a key holding the wrong kind of value", true);

    /** Error reply for a wrong type operation on a HyperLogLog key, the text is aligned with redis. */
    public static final ErrorReply WRONG_TYPE_HLL = new ErrorReply(
            "WRONGTYPE Key is not a valid HyperLogLog string value.", true);

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

    /** Error reply for read-only mode, aligned with Redis' reply text. */
    public static final ErrorReply READONLY = new ErrorReply("READONLY You can't write against a read only replica.", true);

    /** Error reply for cannot read. */
    public static final ErrorReply CANNOT_READ = new ErrorReply("cannot read");

    /** Error reply for unsupported operation. */
    public static final ErrorReply NOT_SUPPORT = new ErrorReply("operation not supported");

    /** Error reply for malformed binary payload, e.g. a BF.LOADCHUNK data chunk. */
    public static final ErrorReply BAD_DATA_FORMAT = new ErrorReply("bad data format");

    /** Error reply for a bloom filter capacity that is not greater than zero. */
    public static final ErrorReply CAPACITY_NOT_POSITIVE = new ErrorReply("capacity must be > 0");

    /** Error reply for unsupported dump type. */
    public static final ErrorReply DUMP_TYPE_NOT_SUPPORT = new ErrorReply("dump type not support");

    /** Error reply for an integer overflow when increment or decrement a value. */
    public static final ErrorReply INCR_DECR_OVERFLOW = new ErrorReply("increment or decrement would overflow");

    /** Error reply for a debug sub command that is not supported. */
    public static final ErrorReply DEBUG_SUB_COMMAND_NOT_SUPPORT = new ErrorReply("debug subcommand not support");

    /** Error reply for cluster slot crossing shards, the reply is CROSSSLOT prefixed, the text is aligned with redis. */
    public static final ErrorReply CLUSTER_SLOT_CROSS_SHARDS = new ErrorReply(
            "CROSSSLOT Keys in request don't hash to the same slot", true);

    /** Error reply for cluster slot not set, the reply is CLUSTERDOWN prefixed, the text is aligned with redis. */
    public static final ErrorReply CLUSTER_SLOT_NOT_SET = new ErrorReply(
            "CLUSTERDOWN Hash slot not served", true);

    /** Error reply for file does not exist. */
    public static final ErrorReply NO_SUCH_FILE = new ErrorReply("no such file");

    /** Error reply for the redis BUSYKEY error, the reply is BUSYKEY prefixed. */
    public static final ErrorReply BUSYKEY = new ErrorReply("BUSYKEY Target key name already exists.", true);

    /** Error reply for an unsupported RESP protocol version, the reply is NOPROTO prefixed, the text is aligned with redis. */
    public static final ErrorReply NOPROTO = new ErrorReply("NOPROTO unsupported protocol version", true);

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

    /**
     * @return the error message of this reply
     */
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
    public boolean equals(Object obj) {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        var that = (ErrorReply) obj;
        return rawErrorPrefix == that.rawErrorPrefix && Objects.equals(message, that.message);
    }

    @Override
    public int hashCode() {
        return Objects.hash(message, rawErrorPrefix);
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
