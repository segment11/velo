package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.ConfForGlobal;
import org.jetbrains.annotations.TestOnly;

import java.math.BigDecimal;
import java.math.RoundingMode;

/**
 * Represents a RESP3 double reply. This class encapsulates a double value using a BigDecimal
 * to maintain precision and provides methods to serialize it to RESP3 format.
 */
public class DoubleReply implements Reply {

    /**
     * Marker byte for RESP3 double type.
     */
    private static final byte DOUBLE_MARKER = ',';

    // TODO: Add support for inf/-inf

    /**
     * The value of the double reply. Stored as a BigDecimal for precision.
     */
    private final BigDecimal value;

    /**
     * Constructs a new DoubleReply with the specified BigDecimal value.
     *
     * @param value The BigDecimal value to be stored in the DoubleReply.
     */
    public DoubleReply(BigDecimal value) {
        this.value = value;
    }

    /**
     * Returns the double value of this DoubleReply object.
     * This method is intended for testing purposes.
     *
     * @return The double value.
     */
    @TestOnly
    public double doubleValue() {
        return value.doubleValue();
    }

    /**
     * Returns a ByteBuf representation of this DoubleReply object.
     * The double value is scaled according to the global configuration and
     * then converted to a plain string. The string is wrapped in a BulkReply
     * object and its buffer is returned.
     *
     * @return ByteBuf representation of the DoubleReply.
     */
    @Override
    public ByteBuf buffer() {
        var scaled = value.setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
        var str = scaled.toPlainString();
        return new BulkReply(str.getBytes()).buffer();
    }

    /**
     * Returns a ByteBuf representation of this DoubleReply object in RESP3 format.
     * The double marker is prepended to the scaled double value, followed by CRLF (\r\n).
     * The result is wrapped in a ByteBuf object and returned.
     *
     * @return ByteBuf representation of the DoubleReply in RESP3 format.
     */
    @Override
    public ByteBuf bufferAsResp3() {
        var scaled = value.setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
        var str = scaled.toPlainString();
        int len = 1 + str.length() + 2; // 1 byte for the marker, length of the string, 2 bytes for CRLF

        var bytes = new byte[len];
        var bb = ByteBuf.wrapForWriting(bytes);
        bb.writeByte(DOUBLE_MARKER);
        bb.put(str.getBytes());
        bb.put(BulkReply.CRLF);
        return bb;
    }
}