package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.CompressedValue;

/**
 * Represents an error reply in the Velocity protocol.
 * This class encapsulates various error messages that can be returned by the server.
 */
public class ErrorReply implements Reply {

    /**
     * Error reply indicating a format error.
     */
    public static final ErrorReply FORMAT = new ErrorReply("format");

    /**
     * Generates a wrong number of arguments error reply for a given command.
     *
     * @param cmd given command
     * @return the error reply
     */
    public static ErrorReply WRONG_NUMBER(String cmd) {
        return new ErrorReply("*wrong number of arguments for '" + cmd + "' command");
    }

    /**
     * Error reply indicating that a dictionary is missing.
     */
    public static final ErrorReply DICT_MISSING = new ErrorReply("dict missing");

    /**
     * Error reply indicating that a key is too long.
     */
    public static final ErrorReply KEY_TOO_LONG = new ErrorReply("key too long (max length is " + CompressedValue.KEY_MAX_LENGTH + ")");

    /**
     * Error reply indicating that a value is too long.
     */
    public static final ErrorReply VALUE_TOO_LONG = new ErrorReply("value too long (max length is " + CompressedValue.VALUE_MAX_LENGTH + ")");

    /**
     * Error reply indicating that the server has stopped.
     */
    public static final ErrorReply SERVER_STOPPED = new ErrorReply("server stopped");

    /**
     * Error reply indicating authentication failure.
     */
    public static final ErrorReply AUTH_FAILED = new ErrorReply("auth failed !WRONGPASS!");

    /**
     * Error reply indicating that no authentication was provided.
     */
    public static final ErrorReply NO_AUTH = new ErrorReply("no auth");

    /**
     * Error reply indicating that the user ACL permit limit has been reached.
     */
    public static final ErrorReply ACL_PERMIT_LIMIT = new ErrorReply("user acl permit limit !NOPERM!");

    /**
     * Error reply indicating that the user ACL permit limit has been reached for a specific key.
     */
    public static final ErrorReply ACL_PERMIT_KEY_LIMIT = new ErrorReply("user acl permit limit !NOPERM! !key!");

    /**
     * Error reply indicating an invalid ACL SETUSER modifier.
     */
    public static final ErrorReply ACL_SETUSER_RULE_INVALID = new ErrorReply("!Error in ACL SETUSER modifier!");

    /**
     * Error reply indicating that a Bloom filter already exists.
     */
    public static final ErrorReply BF_ALREADY_EXISTS = new ErrorReply("bloom filter already exists");

    /**
     * Error reply indicating that a Bloom filter does not exist.
     */
    public static final ErrorReply BF_NOT_EXISTS = new ErrorReply("bloom filter not exists");

    /**
     * Error reply indicating a syntax error.
     */
    public static final ErrorReply SYNTAX = new ErrorReply("syntax error");

    /**
     * Error reply indicating that a value is not an integer.
     */
    public static final ErrorReply NOT_INTEGER = new ErrorReply("not integer");

    /**
     * Error reply indicating that a value is not a float.
     */
    public static final ErrorReply NOT_FLOAT = new ErrorReply("not float");

    /**
     * Error reply indicating that a value is not a string.
     */
    public static final ErrorReply NOT_STRING = new ErrorReply("not string");

    /**
     * Error reply indicating an invalid integer.
     */
    public static final ErrorReply INVALID_INTEGER = new ErrorReply("invalid integer");

    /**
     * Error reply indicating that a range is out of index.
     */
    public static final ErrorReply RANGE_OUT_OF_INDEX = new ErrorReply("range out of index");

    /**
     * Error reply indicating that a key does not exist.
     */
    public static final ErrorReply NO_SUCH_KEY = new ErrorReply("not such key");

    /**
     * Error reply indicating that the wrong type was used.
     */
    public static final ErrorReply WRONG_TYPE = new ErrorReply("wrong type");

    /**
     * Error reply indicating that a list size is too long.
     */
    public static final ErrorReply LIST_SIZE_TO_LONG = new ErrorReply("list size too long");

    /**
     * Error reply indicating that a hash size is too long.
     */
    public static final ErrorReply HASH_SIZE_TO_LONG = new ErrorReply("hash size too long");

    /**
     * Error reply indicating that a set size is too long.
     */
    public static final ErrorReply SET_SIZE_TO_LONG = new ErrorReply("set size too long");

    /**
     * Error reply indicating that a set member length is too long.
     */
    public static final ErrorReply SET_MEMBER_LENGTH_TO_LONG = new ErrorReply("set member length too long");

    /**
     * Error reply indicating that a zset size is too long.
     */
    public static final ErrorReply ZSET_SIZE_TO_LONG = new ErrorReply("zset size too long");

    /**
     * Error reply indicating that a zset member length is too long.
     */
    public static final ErrorReply ZSET_MEMBER_LENGTH_TO_LONG = new ErrorReply("zset member length too long");

    /**
     * Error reply indicating that an index is out of range.
     */
    public static final ErrorReply INDEX_OUT_OF_RANGE = new ErrorReply("index out of range");

    /**
     * Error reply indicating that the server is in read-only mode.
     */
    public static final ErrorReply READONLY = new ErrorReply("readonly");

    /**
     * Error reply indicating that a feature or operation is not supported.
     */
    public static final ErrorReply NOT_SUPPORT = new ErrorReply("not support");

    /**
     * Error reply indicating that a dump type is not supported.
     */
    public static final ErrorReply DUMP_TYPE_NOT_SUPPORT = new ErrorReply("dump type not support");

    /**
     * Error reply indicating that a cluster slot crosses shards.
     */
    public static final ErrorReply CLUSTER_SLOT_CROSS_SHARDS = new ErrorReply("cluster slot cross shards");

    /**
     * Error reply indicating that a cluster slot is not set.
     */
    public static final ErrorReply CLUSTER_SLOT_NOT_SET = new ErrorReply("cluster slot not set");

    /**
     * Error reply indicating that a file does not exist.
     */
    public static final ErrorReply NO_SUCH_FILE = new ErrorReply("no such file");

    /**
     * Error reply indicating that a target key name is busy.
     */
    public static final ErrorReply TARGET_KEY_BUSY = new ErrorReply("target key name is busy");

    /**
     * Creates a new ErrorReply for a MOVED error, indicating that the client should move to a different node.
     *
     * @param toClientSlot The slot number to which the client should move.
     * @param host         The host of the new node.
     * @param port         The port of the new node.
     * @return A new ErrorReply with the MOVED message.
     */
    public static ErrorReply clusterMoved(int toClientSlot, String host, int port) {
        return new ErrorReply("MOVED " + toClientSlot + " " + host + ":" + port);
    }

    private final String message;

    /**
     * Constructs a new ErrorReply with the specified error message.
     *
     * @param message The error message to be returned.
     */
    public ErrorReply(String message) {
        this.message = message;
    }

    /**
     * Returns the error message associated with this ErrorReply.
     *
     * @return The error message.
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

    /**
     * Returns a ByteBuf containing the error message formatted for the Velocity protocol.
     *
     * @return A ByteBuf with the formatted error message.
     */
    @Override
    public ByteBuf buffer() {
        var bytes = ("-ERR " + message + "\r\n").getBytes();
        return ByteBuf.wrapForReading(bytes);
    }

    /**
     * Returns a ByteBuf containing the error message suitable for HTTP responses.
     *
     * @return A ByteBuf with the error message.
     */
    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(message.getBytes());
    }
}