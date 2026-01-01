package io.velo.decode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;
import io.velo.ConfForGlobal;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * A class to decode RESP (Redis Serialization Protocol) messages using Netty's ByteBuf.
 * This implementation is adapted from Camellia Redis Proxy, specifically from the CommandDecoder class.
 */
public class RESP {
    // MARKERS for different types of RESP messages
    private static final byte STRING_MARKER = '+';
    private static final byte BYTES_MARKER = '$';
    private static final byte ARRAY_MARKER = '*';

    // Maximum length of a positive long in characters
    private static final int POSITIVE_LONG_MAX_LENGTH = 19; // length of Long.MAX_VALUE

    /**
     * A private static class that implements ByteProcessor to process numeric values from ByteBuf.
     */
    private static final class NumberProcessor implements ByteProcessor {
        private int result;

        /**
         * Process a byte to form a number.
         *
         * @param value the byte value to process
         * @return true to continue processing, false to stop
         * @throws IllegalArgumentException if the byte value is not a digit.
         */
        @Override
        public boolean process(byte value) {
            if (value < '0' || value > '9') {
                throw new IllegalArgumentException("Bad byte in number=" + value);
            }
            result = result * 10 + (value - '0');
            return true;
        }

        /**
         * Get the processed number.
         *
         * @return the processed number
         */
        public int content() {
            return result;
        }

        /**
         * Reset the number processor.
         */
        public void reset() {
            result = 0;
        }
    }

    private final NumberProcessor numberProcessor = new NumberProcessor();

    private static final Logger log = LoggerFactory.getLogger(RESP.class);

    /**
     * Parse a Redis number from a ByteBuf.
     *
     * @param in the ByteBuf containing the number
     * @return the parsed integer
     * @throws IllegalArgumentException if the number is malformed or too large.
     */
    private int parseRedisNumber(ByteBuf in) {
        final int readableBytes = in.readableBytes();
        final boolean negative = readableBytes > 0 && in.getByte(in.readerIndex()) == '-';
        final int extraOneByteForNegative = negative ? 1 : 0;
        if (readableBytes <= extraOneByteForNegative) {
            throw new IllegalArgumentException("No number to parse=" + in.toString(CharsetUtil.US_ASCII));
        }
        if (readableBytes > POSITIVE_LONG_MAX_LENGTH + extraOneByteForNegative) {
            throw new IllegalArgumentException("Too many characters to be a valid RESP Integer=" +
                    in.toString(CharsetUtil.US_ASCII));
        }
        if (negative) {
            numberProcessor.reset();
            in.skipBytes(extraOneByteForNegative);
            in.forEachByte(numberProcessor);
            return -1 * numberProcessor.content();
        }
        numberProcessor.reset();
        in.forEachByte(numberProcessor);
        return numberProcessor.content();
    }

    /**
     * Read a line from the ByteBuf until the CR LF sequence.
     *
     * @param in the ByteBuf to read from
     * @return the ByteBuf slice containing the read line without the CR LF sequence, or null if a complete line is not available
     */
    private ByteBuf readLine(ByteBuf in) {
        // \r\n
        if (!in.isReadable(2)) {
            return null;
        }
        final int lfIndex = in.forEachByte(ByteProcessor.FIND_LF);
        if (lfIndex < 0) {
            return null;
        }
        var data = in.readSlice(lfIndex - in.readerIndex() - 1); // `-1` is for CR
        in.skipBytes(2);
        return data;
    }

    BigStringNoMemoryCopy bigStringNoMemoryCopy = new BigStringNoMemoryCopy();

    /**
     * Decode a RESP message from a ByteBuf.
     *
     * @param bb the ByteBuf containing the RESP message
     * @return the 2D byte array where each element is a RESP message
     * @throws IllegalArgumentException if the RESP message format is incorrect.
     */
    public byte[][] decode(ByteBuf bb) {
        String cmd = null;
        int beforeReadReaderIndex = bb.readerIndex();

        byte[][] bytes = null;
        outerLoop:
        while (true) {
            if (bytes == null) {
                if (bb.readableBytes() <= 0) {
                    break;
                }
                int readerIndex = bb.readerIndex();
                byte b = bb.readByte();
                if (b == STRING_MARKER || b == ARRAY_MARKER) {
                    var lineBuf = readLine(bb);
                    if (lineBuf == null) {
                        bb.readerIndex(readerIndex);
                        break;
                    }
                    int number = parseRedisNumber(lineBuf);
                    bytes = new byte[number][];
                } else {
                    throw new IllegalArgumentException("Unexpected character=" + b);
                }
            } else {
                int numArgs = bytes.length;
                for (int i = 0; i < numArgs; i++) {
                    if (bb.readableBytes() <= 0) {
                        break outerLoop;
                    }
                    int readerIndex = bb.readerIndex();
                    byte b = bb.readByte();
                    if (b == BYTES_MARKER) {
                        var lineBuf = readLine(bb);
                        if (lineBuf == null) {
                            bb.readerIndex(readerIndex);
                            break outerLoop;
                        }
                        int size = parseRedisNumber(lineBuf);
                        if (bb.readableBytes() < size + 2) {
                            bb.readerIndex(readerIndex);
                            break outerLoop;
                        }

                        // for no memory copy
                        if (size >= ConfForGlobal.bigStringNoMemoryCopySize && bb instanceof CompositeByteBuf cbb && cbb.numComponents() == 1) {
                            boolean isSetBigString = false;
                            if (cmd != null) {
                                var isSet = cmd.equals("set");
                                var isSetEx = cmd.equals("setex");

                                if (isSet && i == 2) {
                                    bigStringNoMemoryCopy.readBufferInDataIndex = 2;
                                    bytes[2] = cbb.array();
                                    isSetBigString = true;
                                } else if (isSetEx && i == 3) {
                                    bigStringNoMemoryCopy.readBufferInDataIndex = 3;
                                    bytes[3] = cbb.array();
                                    isSetBigString = true;
                                }
                            }

                            if (isSetBigString) {
                                bigStringNoMemoryCopy.offset = cbb.readerIndex() - beforeReadReaderIndex;
                                bigStringNoMemoryCopy.length = size;
                                cbb.skipBytes(size + 2);
                            } else {
                                log.warn("Perf bad, copy big value from read buffer, size={}, cmd={}, index={}", size, cmd, i);
                                bytes[i] = new byte[size];
                                cbb.readBytes(bytes[i]);
                                cbb.skipBytes(2);
                            }
                        } else {
                            bytes[i] = new byte[size];
                            bb.readBytes(bytes[i]);
                            bb.skipBytes(2);

                            if (i == 0) {
                                cmd = new String(bytes[0]).toLowerCase();
                            }
                        }
                    } else {
                        throw new IllegalArgumentException("Unexpected character: " + b);
                    }
                }
                break;
            }
        }
        return bytes;
    }
}