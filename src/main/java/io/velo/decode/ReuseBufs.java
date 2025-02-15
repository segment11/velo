package io.velo.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ByteProcessor;
import org.jetbrains.annotations.TestOnly;

import java.util.Arrays;

import static io.velo.decode.RESP.*;

public class ReuseBufs {
    private static final int MAX_DATA_IN_ONE_REQUEST = 1024;

    int dataLength = 0;
    final int[] offsets = new int[MAX_DATA_IN_ONE_REQUEST];
    final int[] lengths = new int[MAX_DATA_IN_ONE_REQUEST];

    void printForDebug() {
        for (int i = 0; i < dataLength; i++) {
            int offset = offsets[i];
            int length = lengths[i];

            var bodyBytes = new byte[length];
            compositeByteBuf.getBytes(offset, bodyBytes);

            System.out.println("offset=" + offset + ", length=" + length + ", body=" + new String(bodyBytes));
        }
    }

    boolean isFeedFull() {
        if (dataLength == 0 || currentDataIndex == 0) {
            return false;
        }

        return currentDataIndex == dataLength;
    }

    private int head = 0;
    private int bufCountLastSet = 0;
    private int bufCount = 0;

    private final byte[][] arrays = new byte[MAX_DATA_IN_ONE_REQUEST][];
    private final ByteBuf[] nettyByteBufs = new ByteBuf[MAX_DATA_IN_ONE_REQUEST];

    CompositeByteBuf compositeByteBuf;

    private void reuseNettyByteBufs() {
        ByteBuf[] nettyByteBufsRefCopy = bufCountLastSet != 0 ? Arrays.copyOf(nettyByteBufs, bufCountLastSet) : null;

        boolean isNeedRecreateCompositeByteBuf = false;
        for (int i = 0; i < bufCount; i++) {
            var array = arrays[i];
            var preferReuseOne = nettyByteBufs[i];
            if (preferReuseOne == null) {
                nettyByteBufs[i] = Unpooled.wrappedBuffer(array);
                isNeedRecreateCompositeByteBuf = true;
            } else {
                if (preferReuseOne.array() == array) {
                    continue;
                }

                isNeedRecreateCompositeByteBuf = true;
                if (nettyByteBufsRefCopy == null) {
                    nettyByteBufs[i] = Unpooled.wrappedBuffer(array);
                } else {
                    boolean isSet = false;
                    for (int j = 0; j < bufCountLastSet; j++) {
                        var lastSet = nettyByteBufsRefCopy[j];
                        if (lastSet.array() == array) {
                            nettyByteBufs[i] = lastSet;
                            isSet = true;
                            break;
                        }
                    }
                    if (!isSet) {
                        nettyByteBufs[i] = Unpooled.wrappedBuffer(array);
                    }
                }
            }
        }

        if (compositeByteBuf == null || isNeedRecreateCompositeByteBuf) {
            compositeByteBuf = Unpooled.compositeBuffer();
            for (int i = 0; i < bufCount; i++) {
                compositeByteBuf.addComponent(true, nettyByteBufs[i]);
            }
        }
    }

    private static final int POSITIVE_INTEGER_MAX_LENGTH = 10;

    private int afterScanOrSkipByteCount = 0;
    private boolean isNotSetOffsetAndLength = true;

    private static class FindUntilLF implements ByteProcessor {
        private final byte[] intStringBytes = new byte[POSITIVE_INTEGER_MAX_LENGTH];
        private int index = 0;

        int parseInt() {
            return Integer.parseInt(new String(intStringBytes, 0, index));
        }

        @Override
        public boolean process(byte b) {
            if (b == 13) {
                return false;
            }

            if (index >= POSITIVE_INTEGER_MAX_LENGTH) {
                throw new IllegalArgumentException("Too many characters to be a valid RESP Integer, integer length >=" + index);
            }

            if (b < '0' || b > '9') {
                throw new IllegalArgumentException("Bad byte in number=" + b);
            }

            intStringBytes[index] = b;

            index++;
            return true;
        }

        void reset() {
            index = 0;
        }
    }

    private final FindUntilLF findUntilLF = new FindUntilLF();

    private int parseDataLength() throws MalformedDataException {
        // \r\n
        if (!compositeByteBuf.isReadable(2)) {
            return -1;
        }

        findUntilLF.reset();
        var start = compositeByteBuf.forEachByte(findUntilLF);
        if (start == -1) {
            return -1;
        }
        var index = findUntilLF.index;
        dataLength = findUntilLF.parseInt();

        if (compositeByteBuf.readableBytes() < index + 2) {
            return -1;
        }
        compositeByteBuf.skipBytes(index + 2);

        afterScanOrSkipByteCount += index + 2;
        return index;
    }

    private int currentDataIndex = 0;

    private int readCurrentDataOffsetAndLength() throws MalformedDataException {
        // \r\n
        if (!compositeByteBuf.isReadable(2)) {
            return -1;
        }

        findUntilLF.reset();
        var start = compositeByteBuf.forEachByte(findUntilLF);
        if (start == -1) {
            return -1;
        }
        var index = findUntilLF.index;
        var bodyLength = findUntilLF.parseInt();

        if (compositeByteBuf.readableBytes() < index + 2) {
            return -1;
        }
        compositeByteBuf.skipBytes(index + 2);

        if (compositeByteBuf.readableBytes() < bodyLength + 2) {
            return -1;
        }
        compositeByteBuf.skipBytes(bodyLength + 2);

        lengths[currentDataIndex] = bodyLength;
        offsets[currentDataIndex] = afterScanOrSkipByteCount + index + 2;
        currentDataIndex++;

        afterScanOrSkipByteCount += index + 2 + bodyLength + 2;
        return index;
    }

    void parseOffsetsAndLengths() throws MalformedDataException {
        outerLoop:
        while (true) {
            if (isNotSetOffsetAndLength) {
                if (compositeByteBuf.readableBytes() <= 0) {
                    break;
                }

                byte b = compositeByteBuf.readByte();
                afterScanOrSkipByteCount++;
                if (b == STRING_MARKER || b == ARRAY_MARKER) {
                    int r = parseDataLength();
                    if (r == -1) {
                        break;
                    }
                    isNotSetOffsetAndLength = false;
                } else {
                    throw new IllegalArgumentException("Unexpected character=" + b);
                }
            } else {
                for (int i = 0; i < dataLength; i++) {
                    if (compositeByteBuf.readableBytes() <= 0) {
                        break outerLoop;
                    }
                    byte b = compositeByteBuf.readByte();
                    afterScanOrSkipByteCount++;
                    if (b == BYTES_MARKER) {
                        var r = readCurrentDataOffsetAndLength();
                        if (r == -1) {
                            break outerLoop;
                        }
                    } else {
                        throw new IllegalArgumentException("Unexpected character=" + b);
                    }
                }
                break;
            }
        }
    }

    @TestOnly
    void decodeFromBufs(ByteBufs bufs) throws MalformedDataException {
        refresh(bufs);
        parseOffsetsAndLengths();
    }

    public void refresh(ByteBufs bufs) {
        int innerArrayIndex = 0;
        for (var buf : bufs) {
            if (innerArrayIndex == 0) {
                this.head = buf.head();
            }
            this.arrays[innerArrayIndex] = buf.array();
            innerArrayIndex++;
        }
        this.bufCount = innerArrayIndex;
        reuseNettyByteBufs();
        this.compositeByteBuf.readerIndex(this.head).writerIndex(this.head + bufs.remainingBytes());
        this.bufCountLastSet = bufCount;

        this.dataLength = 0;
        this.currentDataIndex = 0;
        this.afterScanOrSkipByteCount = this.head;
        this.isNotSetOffsetAndLength = true;
    }
}
