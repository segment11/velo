package io.velo.decode;

import io.netty.buffer.ByteBuf;
import io.netty.util.ByteProcessor;
import io.netty.util.CharsetUtil;

// reuse decode by netty ByteBuf, copy from camellia-redis-proxy com.netease.nim.camellia.redis.proxy.netty.CommandDecoder
public class RESP {
    static final byte STRING_MARKER = '+';
    static final byte BYTES_MARKER = '$';
    static final byte ARRAY_MARKER = '*';

    private static final int POSITIVE_LONG_MAX_LENGTH = 19; // length of Long.MAX_VALUE

    private static final class NumberProcessor implements ByteProcessor {
        private int result;

        @Override
        public boolean process(byte value) {
            if (value < '0' || value > '9') {
                throw new IllegalArgumentException("Bad byte in number=" + value);
            }
            result = result * 10 + (value - '0');
            return true;
        }

        public int content() {
            return result;
        }

        public void reset() {
            result = 0;
        }
    }

    private final NumberProcessor numberProcessor = new NumberProcessor();

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

    public byte[][] decode(ByteBuf bb) {
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
                        if (bb.readableBytes() >= size + 2) {
                            bytes[i] = new byte[size];
                            bb.readBytes(bytes[i]);
                            bb.skipBytes(2);
                        } else {
                            bb.readerIndex(readerIndex);
                            break outerLoop;
                        }
                    } else {
                        throw new IllegalArgumentException("Unexpected character： " + b);
                    }
                }
                break;
            }
        }
        return bytes;
    }
}
