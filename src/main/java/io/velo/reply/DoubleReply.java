package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import org.jetbrains.annotations.TestOnly;

import java.math.BigDecimal;

/**
 * RESP3 double reply backed by a BigDecimal for precision.
 */
public class DoubleReply implements Reply {

    private static final byte DOUBLE_MARKER = ',';

    private final BigDecimal value;

    /**
     * @param value the BigDecimal value
     */
    public DoubleReply(BigDecimal value) {
        this.value = value;
    }

    /**
     * @return the double value of this reply
     */
    @TestOnly
    public double doubleValue() {
        return value.doubleValue();
    }

    /**
     * Format like redis does for incrbyfloat-family replies: the decimal value as-is,
     * with trailing zeros stripped and no scientific notation. The number.doubleScale
     * config is not applied here (bug 79.1).
     */
    private String toRedisFormat() {
        var stripped = value.stripTrailingZeros();
        if (stripped.scale() < 0) {
            // a value like 1E+3 is 1000
            stripped = stripped.setScale(0);
        }
        return stripped.toPlainString();
    }

    @Override
    public ByteBuf buffer() {
        return new BulkReply(toRedisFormat()).buffer();
    }

    @Override
    public ByteBuf bufferAsResp3() {
        var str = toRedisFormat();
        int len = 1 + str.length() + 2; // 1 byte for the marker, length of the string, 2 bytes for CRLF

        var bytes = new byte[len];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeByte(DOUBLE_MARKER);
        buf.put(str.getBytes());
        buf.put(BulkReply.CRLF);
        return buf;
    }
}