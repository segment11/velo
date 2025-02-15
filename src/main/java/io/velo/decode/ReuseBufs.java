package io.velo.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ByteProcessor;
import org.jetbrains.annotations.TestOnly;

import static io.velo.decode.RESP.*;

public class ReuseBufs {
    private static final int MAX_DATA_IN_ONE_REQUEST = 1024;

    private int dataLength = 0;
    private final int[] offsets = new int[MAX_DATA_IN_ONE_REQUEST];
    private final int[] lengths = new int[MAX_DATA_IN_ONE_REQUEST];

    @TestOnly
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
    private int bufCount = 0;

    private final byte[][] arrays = new byte[MAX_DATA_IN_ONE_REQUEST][];

    private int nettyByteBufsCount = 0;
    private final ByteBuf[] nettyByteBufs = new ByteBuf[MAX_DATA_IN_ONE_REQUEST];
    private final CompositeByteBuf[] compositeByteBufs = new CompositeByteBuf[MAX_DATA_IN_ONE_REQUEST];

    CompositeByteBuf compositeByteBuf;

    private void reuseNettyByteBufs() {
        int[] indexArray = new int[MAX_DATA_IN_ONE_REQUEST];

        for (int i = 0; i < bufCount; i++) {
            var array = arrays[i];

            for (int j = 1; j <= nettyByteBufsCount; j++) {
                var lastSet = nettyByteBufs[j];
                if (lastSet.array() == array) {
                    indexArray[i] = j;
                    break;
                }
            }
        }

        for (int i = 0; i < bufCount; i++) {
            int j = indexArray[i];
            if (j == 0) {
                var array = arrays[i];

                int nextIndex = nettyByteBufsCount + 1;
                nettyByteBufs[nextIndex] = Unpooled.wrappedBuffer(array);
                compositeByteBufs[nextIndex] = Unpooled.compositeBuffer();
                compositeByteBufs[nextIndex].addComponent(true, nettyByteBufs[nextIndex]);

                indexArray[i] = nextIndex;
                nettyByteBufsCount = nextIndex;
            }
        }

        if (bufCount == 1) {
            compositeByteBuf = compositeByteBufs[indexArray[0]];
        } else {
            compositeByteBuf = Unpooled.compositeBuffer();
            for (int i = 0; i < bufCount; i++) {
                int j = indexArray[i];
                compositeByteBuf.addComponent(true, nettyByteBufs[j]);
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

    private int parseDataLength() {
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

    private int readCurrentDataOffsetAndLength() {
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

    void refresh(ByteBufs bufs) {
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

        this.dataLength = 0;
        this.currentDataIndex = 0;
        this.afterScanOrSkipByteCount = this.head;
        this.isNotSetOffsetAndLength = true;
    }
}
