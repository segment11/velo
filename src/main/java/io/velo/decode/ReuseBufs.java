package io.velo.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.CompositeByteBuf;
import io.netty.buffer.Unpooled;
import io.velo.repl.Repl;

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

    private ByteBufs bufs;
    private int head = 0;
    private int bufCountLastSet = 0;
    private int bufCount = 0;
    private final byte[][] arrays = new byte[MAX_DATA_IN_ONE_REQUEST][];
    private final ByteBuf[] nettyByteBufs = new ByteBuf[MAX_DATA_IN_ONE_REQUEST];

    private CompositeByteBuf compositeByteBuf;

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

    private final byte[] httpHeaderMethodBytes = new byte[6];
    boolean isHttp = false;
    boolean isRepl = false;

    private static final int POSITIVE_INTEGER_MAX_LENGTH = 10;

    private int afterScanOrSkipByteCount = 0;
    private boolean isNotSetOffsetAndLength = true;

    private int parseDataLength() throws MalformedDataException {
        // \r\n
        if (!bufs.hasRemainingBytes(2)) {
            return -1;
        }

        final byte[] intStringBytes = new byte[POSITIVE_INTEGER_MAX_LENGTH];
        final int[] lfIndex = {-1};
        bufs.scanBytes(0, (index, b) -> {
            if (b == 13) {
                lfIndex[0] = index;
                return true;
            }

            if (index >= POSITIVE_INTEGER_MAX_LENGTH) {
                throw new IllegalArgumentException("Too many characters to be a valid RESP Integer=" + bufs);
            }

            if (b < '0' || b > '9') {
                throw new IllegalArgumentException("Bad byte in number=" + b);
            }

            intStringBytes[index] = b;
            return false;
        });

        var index = lfIndex[0];
        if (index == -1) {
            return -1;
        }

        dataLength = Integer.parseInt(new String(intStringBytes, 0, index));
        // + \r\n 2 bytes
        bufs.skip(index + 2);
        afterScanOrSkipByteCount += index + 2;
        return index;
    }

    private int currentDataIndex = 0;

    private int readCurrentDataOffsetAndLength() throws MalformedDataException {
        // \r\n
        if (!bufs.hasRemainingBytes(2)) {
            return -1;
        }

        final byte[] intStringBytes = new byte[POSITIVE_INTEGER_MAX_LENGTH];
        final int[] lfBodyLengthIndex = {-1};
        bufs.scanBytes(0, (index, b) -> {
            if (b == 13) {
                lfBodyLengthIndex[0] = index;
                return true;
            }

            if (index >= POSITIVE_INTEGER_MAX_LENGTH) {
                throw new IllegalArgumentException("Too many characters to be a valid RESP Integer=" + bufs);
            }

            if (b < '0' || b > '9') {
                throw new IllegalArgumentException("Bad byte in number=" + b);
            }

            intStringBytes[index] = b;
            return false;
        });

        var index = lfBodyLengthIndex[0];
        if (index == -1) {
            return -1;
        }

        var bodyLength = Integer.parseInt(new String(intStringBytes, 0, index));
        // + \r\n 2 bytes
        bufs.skip(index + 2);

        if (bufs.remainingBytes() < bodyLength + 2) {
            return -1;
        }

        bufs.skip(bodyLength + 2);

        lengths[currentDataIndex] = bodyLength;
        offsets[currentDataIndex] = afterScanOrSkipByteCount + index + 2;
        currentDataIndex++;

        afterScanOrSkipByteCount += index + 2 + bodyLength + 2;
        return index;
    }

    private void parseOffsetsAndLengths() throws MalformedDataException {
        outerLoop:
        while (true) {
            if (isNotSetOffsetAndLength) {
                if (bufs.remainingBytes() <= 0) {
                    break;
                }

                byte b = bufs.getByte();
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
                    if (bufs.remainingBytes() <= 0) {
                        break outerLoop;
                    }
                    byte b = bufs.getByte();
                    afterScanOrSkipByteCount++;
                    if (b == BYTES_MARKER) {
                        var r = readCurrentDataOffsetAndLength();
                        if (r == -1) {
                            break outerLoop;
                        }
                    } else {
                        throw new IllegalArgumentException("Unexpected character： " + b);
                    }
                }
                break;
            }
        }
    }

    public void decodeFromBufs(ByteBufs bufs) throws MalformedDataException {
        this.bufs = bufs;
        int innerArrayIndex = 0;
        for (var buf : bufs) {
            if (innerArrayIndex == 0) {
                head = buf.head();
            }
            arrays[innerArrayIndex++] = buf.array();
        }
        this.bufCount = innerArrayIndex;
        reuseNettyByteBufs();
        this.compositeByteBuf.readerIndex(this.head);
        this.bufCountLastSet = bufCount;

        this.dataLength = 0;
        this.currentDataIndex = 0;
        this.afterScanOrSkipByteCount = this.head;
        this.isNotSetOffsetAndLength = true;

        this.isHttp = false;
        this.isRepl = false;

        if (bufs.remainingBytes() < 6) {
            parseOffsetsAndLengths();
        } else {
            bufs.scanBytes(0, (index, b) -> {
                if (index == 6) {
                    return true;
                }

                httpHeaderMethodBytes[index] = b;
                return false;
            });

            var isGet = Arrays.equals(httpHeaderMethodBytes, 0, 3, HttpHeaderBody.GET, 0, 3);
            var isPost = Arrays.equals(httpHeaderMethodBytes, 0, 4, HttpHeaderBody.POST, 0, 4);
            var isPut = Arrays.equals(httpHeaderMethodBytes, 0, 3, HttpHeaderBody.PUT, 0, 3);
            var isDelete = Arrays.equals(httpHeaderMethodBytes, 0, 6, HttpHeaderBody.DELETE, 0, 6);
            isHttp = isGet || isPost || isPut || isDelete;
            isRepl = Arrays.equals(httpHeaderMethodBytes, 0, 6, Repl.PROTOCOL_KEYWORD_BYTES, 0, 6);

            if (isHttp) {

            } else if (isRepl) {

            } else {
                parseOffsetsAndLengths();
            }
        }
    }
}
