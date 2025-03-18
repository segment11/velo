package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

/**
 * Represents a bulk reply in the Redis protocol.
 * A bulk reply is used to send data in a binary-safe format.
 */
public class BulkReply implements Reply {
    private byte[] raw;

    /**
     * Returns the raw bytes of the bulk reply.
     *
     * @return the raw bytes
     */
    public byte[] getRaw() {
        return raw;
    }

    /**
     * Default constructor for BulkReply.
     */
    public BulkReply() {
    }

    /**
     * Constructs a BulkReply with the specified raw bytes.
     *
     * @param raw the raw bytes of the reply
     */
    public BulkReply(byte[] raw) {
        this.raw = raw;
    }

    /**
     * Constructs a BulkReply with the specified long value.
     * The long value is converted to a string and then to bytes.
     *
     * @param l the long value
     */
    public BulkReply(long l) {
        this.raw = String.valueOf(l).getBytes();
    }

    /**
     * Constructs a BulkReply with the specified double value.
     * The double value is converted to a string and then to bytes.
     *
     * @param d the double value
     */
    public BulkReply(double d) {
        this.raw = String.valueOf(d).getBytes();
    }

    /**
     * A BulkReply instance representing the number 0.
     */
    public static final BulkReply ZERO = new BulkReply("0".getBytes());

    private static final char CR = '\r';
    private static final char LF = '\n';

    /**
     * Marker byte for the start of a bulk reply in the Redis protocol.
     */
    public static final byte MARKER = '$';
    public static final byte MARKER_RESP3 = '+';

    public static final byte[] CRLF = new byte[]{CR, LF};

    /**
     * Bytes representing -1 in a bulk reply format.
     */
    public static final byte[] NEG_ONE = convert(-1, false);

    /**
     * Bytes representing -1 in a bulk reply format with CRLF.
     */
    public static final byte[] NEG_ONE_WITH_CRLF = convert(-1, true);

    private static final int NUM_MAP_LENGTH = 256;
    private static final byte[][] numMap = new byte[NUM_MAP_LENGTH][];
    private static final byte[][] numMapWithCRLF = new byte[NUM_MAP_LENGTH][];

    static {
        for (int i = 0; i < NUM_MAP_LENGTH; i++) {
            numMapWithCRLF[i] = convert(i, true);
            numMap[i] = convert(i, false);
        }
    }

    /**
     * Converts a long value to a byte array representing a bulk reply.
     * Optionally appends CRLF to the byte array.
     *
     * @param value    the long value to convert
     * @param withCRLF true if CRLF should be appended
     * @return the byte array representing the bulk reply
     */
    private static byte[] convert(long value, boolean withCRLF) {
        boolean negative = value < 0;
        long abs = Math.abs(value);
        int index = (value == 0 ? 0 : (int) Math.log10(abs)) + (negative ? 2 : 1);
        var bytes = new byte[withCRLF ? index + 2 : index];
        if (withCRLF) {
            bytes[index] = CR;
            bytes[index + 1] = LF;
        }
        if (negative) bytes[0] = '-';
        long next = abs;
        while ((next /= 10) > 0) {
            bytes[--index] = (byte) ('0' + (abs % 10));
            abs = next;
        }
        bytes[--index] = (byte) ('0' + abs);
        return bytes;
    }

    /**
     * Converts a long value to a byte array representing a bulk reply.
     * Optionally appends CRLF to the byte array.
     * Uses a cache for values from 0 to 255 for performance optimization.
     *
     * @param value    the long value to convert
     * @param withCRLF true if CRLF should be appended
     * @return the byte array representing the bulk reply
     */
    static byte[] numToBytes(long value, boolean withCRLF) {
        if (value >= 0 && value < NUM_MAP_LENGTH) {
            int index = (int) value;
            return withCRLF ? numMapWithCRLF[index] : numMap[index];
        } else if (value == -1) {
            return withCRLF ? NEG_ONE_WITH_CRLF : NEG_ONE;
        }
        return convert(value, withCRLF);
    }

    /**
     * Returns a ByteBuf representing the bulk reply in the Redis protocol format.
     * Format: ${size}\r\n{raw}\r\n
     *
     * @return the ByteBuf representing the bulk reply
     */
    @Override
    public ByteBuf buffer() {
        int size = raw != null ? raw.length : -1;
        var sizeBytes = numToBytes(size, true);

        int len = 1 + sizeBytes.length + (size >= 0 ? size + 2 : 0);
        var bytes = new byte[len];
        var bb = ByteBuf.wrapForWriting(bytes);
        bb.writeByte(MARKER);
        bb.write(sizeBytes);
        if (size >= 0) {
            bb.write(raw);
            bb.write(CRLF);
        }
        return bb;
    }

    @Override
    public ByteBuf bufferAsResp3() {
        int size = raw != null ? raw.length : 0;

        int len = 1 + size + 2;
        var bytes = new byte[len];
        var bb = ByteBuf.wrapForWriting(bytes);
        bb.writeByte(MARKER_RESP3);
        if (size != 0) {
            bb.write(raw);
        }
        bb.write(CRLF);
        return bb;
    }

    /**
     * Returns a ByteBuf wrapping the raw bytes of the bulk reply.
     * This is used in the context of HTTP responses.
     *
     * @return the ByteBuf wrapping the raw bytes
     */
    @Override
    public ByteBuf bufferAsHttp() {
        return ByteBuf.wrapForReading(raw);
    }

    /**
     * Dumps the content of the bulk reply to a StringBuilder for testing purposes.
     *
     * @param sb        the StringBuilder to append the dump to
     * @param nestCount the nesting level (unused in this implementation)
     * @return true if the dump was successful
     */
    @TestOnly
    @Override
    public boolean dumpForTest(StringBuilder sb, int nestCount) {
        sb.append("\"").append(new String(raw)).append("\"");
        return true;
    }
}