package io.velo.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.netty.util.ByteProcessor;
import org.jetbrains.annotations.TestOnly;

import static io.velo.decode.RESP.*;

/**
 * A class designed to decode data from {@link ByteBufs} by reusing Netty ByteBufs to avoid unnecessary object creation.
 * It also calculates and stores offsets and lengths of data chunks to efficiently access decoded data later.
 */
public class ReuseBufs {
    /**
     * Maximum number of data chunks in one request.
     */
    private static final int MAX_DATA_IN_ONE_REQUEST = 1024;

    /**
     * Total number of data chunks.
     */
    private int dataLength = 0;

    /**
     * Offsets of each data chunk in the composite ByteBuf.
     */
    private final int[] offsets = new int[MAX_DATA_IN_ONE_REQUEST];

    /**
     * Lengths of each data chunk in the composite ByteBuf.
     */
    private final int[] lengths = new int[MAX_DATA_IN_ONE_REQUEST];

    /**
     * Prints the current state of offsets and lengths for debugging purposes.
     * Should only be used in test scenarios.
     */
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

    /**
     * Checks if all data chunks have been processed.
     *
     * @return true if all data chunks have been processed, false otherwise.
     */
    boolean isFeedFull() {
        if (dataLength == 0 || currentDataIndex == 0) {
            return false;
        }

        return currentDataIndex == dataLength;
    }

    /**
     * Index to track the head of the buffer.
     */
    private int head = 0;

    /**
     * Number of buffers.
     */
    private int bufCount = 0;

    /**
     * Arrays to store the byte arrays from the input ByteBufs.
     */
    private final byte[][] arrays = new byte[MAX_DATA_IN_ONE_REQUEST][];

    /**
     * Number of Netty ByteBufs reused.
     */
    private int nettyByteBufsCount = 0;

    /**
     * Reused Netty ByteBuf instances.
     */
    private final ByteBuf[] nettyByteBufs = new ByteBuf[MAX_DATA_IN_ONE_REQUEST];

    /**
     * Reused composite ByteBuf instances.
     */
    private final CompositeByteBuf[] compositeByteBufs = new CompositeByteBuf[MAX_DATA_IN_ONE_REQUEST];

    /**
     * Composite ByteBuf to store all data chunks.
     */
    CompositeByteBuf compositeByteBuf;

    /**
     * Reuses Netty ByteBufs to avoid creating new ones, reducing garbage collection overhead.
     */
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

    /**
     * Maximum length for a positive integer in RESP format.
     */
    private static final int POSITIVE_INTEGER_MAX_LENGTH = 10;

    /**
     * Byte count after scanning or skipping.
     */
    private int afterScanOrSkipByteCount = 0;

    /**
     * Flag to check if offsets and lengths have been set.
     */
    private boolean isNotSetOffsetAndLength = true;

    /**
     * Helper class to find the length of data until the newline character.
     */
    private static class FindUntilLF implements ByteProcessor {
        /**
         * Byte array to store integer string.
         */
        private final byte[] intStringBytes = new byte[POSITIVE_INTEGER_MAX_LENGTH];
        /**
         * Current index in the integer string byte array.
         */
        private int index = 0;

        /**
         * Parses the accumulated byte array into an integer.
         *
         * @return parsed integer value.
         */
        int parseInt() {
            return Integer.parseInt(new String(intStringBytes, 0, index));
        }

        /**
         * Processes each byte, accumulating until a newline is found.
         *
         * @param b the byte to process.
         * @return true to continue processing, false to stop.
         */
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

        /**
         * Resets the index for reprocessing.
         */
        void reset() {
            index = 0;
        }
    }

    /**
     * Instance of FindUntilLF to find length until newline.
     */
    private final FindUntilLF findUntilLF = new FindUntilLF();

    /**
     * Parses the length of the data from the composite ByteBuf.
     *
     * @return the parsed length if found, -1 otherwise.
     */
    private int parseDataLength() {
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

    /**
     * Current index in data chunks.
     */
    private int currentDataIndex = 0;

    /**
     * Reads the current data's offset and length from the composite ByteBuf.
     *
     * @return the index if read successfully, -1 otherwise.
     */
    private int readCurrentDataOffsetAndLength() {
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

    /**
     * Parses the offsets and lengths of all data chunks in the composite ByteBuf.
     *
     * @throws MalformedDataException if the data is malformed.
     */
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

    /**
     * Decodes data from ByteBufs for testing purposes.
     *
     * @param bufs the input ByteBufs.
     * @throws MalformedDataException if the data is malformed.
     */
    @TestOnly
    void decodeFromBufs(ByteBufs bufs) throws MalformedDataException {
        refresh(bufs);
        parseOffsetsAndLengths();
    }

    /**
     * Refreshes the internal state with new ByteBufs.
     *
     * @param bufs the input ByteBufs.
     */
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