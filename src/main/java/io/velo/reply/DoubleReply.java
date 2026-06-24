package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.ConfForGlobal;
import org.jetbrains.annotations.TestOnly;

import java.math.BigDecimal;
import java.math.RoundingMode;

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

    @Override
    public ByteBuf buffer() {
        var scaled = value.setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
        var str = scaled.toPlainString();
        return new BulkReply(str).buffer();
    }

    @Override
    public ByteBuf bufferAsResp3() {
        var scaled = value.setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
        var str = scaled.toPlainString();
        int len = 1 + str.length() + 2; // 1 byte for the marker, length of the string, 2 bytes for CRLF

        var bytes = new byte[len];
        var buf = ByteBuf.wrapForWriting(bytes);
        buf.writeByte(DOUBLE_MARKER);
        buf.put(str.getBytes());
        buf.put(BulkReply.CRLF);
        return buf;
    }
}