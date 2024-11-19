package io.velo.reply;

import io.activej.bytebuf.ByteBuf;
import io.velo.ConfForGlobal;
import org.jetbrains.annotations.TestOnly;

import java.math.BigDecimal;
import java.math.RoundingMode;

// RESP3 double
public class DoubleReply implements Reply {
    private static final byte DOUBLE_MARKER = ',';

    // already scale up
    private final BigDecimal value;

    public DoubleReply(BigDecimal value) {
        this.value = value;
    }

    @TestOnly
    public double doubleValue() {
        return value.doubleValue();
    }

    @Override
    public ByteBuf buffer() {
        var scaled = value.setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
        var str = scaled.toPlainString();
        // $+size+raw+\r\n
        int len = 1 + str.length() + 2;

        var bytes = new byte[len];
        var bb = ByteBuf.wrapForWriting(bytes);
        bb.writeByte(BulkReply.MARKER);
        bb.put(str.getBytes());
        bb.put(BulkReply.CRLF);
        return bb;
    }

    @Override
    public ByteBuf bufferAsResp3() {
        var scaled = value.setScale(ConfForGlobal.doubleScale, RoundingMode.HALF_UP);
        var str = scaled.toPlainString();
        // ,<value>\r\n
        int len = 1 + str.length() + 2;

        var bytes = new byte[len];
        var bb = ByteBuf.wrapForWriting(bytes);
        bb.writeByte(DOUBLE_MARKER);
        bb.put(str.getBytes());
        bb.put(BulkReply.CRLF);
        return bb;
    }
}
