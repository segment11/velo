package io.velo.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.velo.repl.Repl;
import io.velo.repl.ReplRequest;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;
import java.nio.charset.StandardCharsets;

/**
 * Decoder for incoming requests supporting both HTTP and RESP protocols.
 */
public class RequestDecoder implements ByteBufsDecoder<ArrayList<Request>> {
    // in local thread
    private final RESP resp = new RESP();

    private static boolean isShortPrefix(ByteBuf buf, byte[] target) {
        int readable = buf.readableBytes();
        if (readable > target.length) {
            return false;
        }
        int readerIndex = buf.readerIndex();
        for (int i = 0; i < readable; i++) {
            if (buf.getByte(readerIndex + i) != target[i]) {
                return false;
            }
        }
        return true;
    }

    private static boolean isPotentialShortHttpOrRepl(ByteBuf buf) {
        return isShortPrefix(buf, HttpHeaderBody.GET)
                || isShortPrefix(buf, HttpHeaderBody.POST)
                || isShortPrefix(buf, HttpHeaderBody.PUT)
                || isShortPrefix(buf, HttpHeaderBody.DELETE)
                || isShortPrefix(buf, Repl.PROTOCOL_KEYWORD_BYTES);
    }

    static ByteBuf fromBufs(ByteBufs bufs) {
        io.netty.buffer.CompositeByteBuf compositeByteBuf = Unpooled.compositeBuffer();
        int capacity = 0;
        for (var buf : bufs) {
            int remainingN = buf.readRemaining();
            if (remainingN > 0) {
                capacity += remainingN;
                compositeByteBuf.addComponent(true, Unpooled.wrappedBuffer(buf.array(), buf.head(), remainingN));
            }
        }
        if (capacity == 0) {
            return null;
        }

        compositeByteBuf.readerIndex(0);
        compositeByteBuf.writerIndex(capacity).capacity(capacity);
        return compositeByteBuf;
    }

    private final Logger log = LoggerFactory.getLogger(RequestDecoder.class);

    /**
     * @param bufs the source of bytes to decode
     * @return a decoded Request or null if not enough bytes
     */
    private Request tryDecodeOne(ByteBufs bufs) throws MalformedDataException {
        var buf = fromBufs(bufs);
        if (buf == null) {
            return null;
        }

        // check first 6 bytes, http or repl at least 6 bytes
        byte[][] data;
        boolean isHttp = false;
        boolean isRepl = false;
        ReplRequest replRequest = null;
        Map<String, String> httpHeaders = null;
        if (buf.capacity() < 6) {
            if (buf.getByte(buf.readerIndex()) != '*') {
                if (isPotentialShortHttpOrRepl(buf)) {
                    return null;
                }
                throw new IllegalArgumentException("Unexpected short protocol prefix=" + buf.getByte(buf.readerIndex()));
            }
            data = resp.decode(buf);
            if (data == null) {
                return null;
            }

            // Not all data is available yet; wait for more buffers
            for (var bb : data) {
                if (bb == null) {
                    return null;
                }
            }
        } else {
            var first6 = new byte[6];
            buf.readBytes(first6);
            var isGet = Arrays.equals(first6, 0, 3, HttpHeaderBody.GET, 0, 3);
            var isPost = Arrays.equals(first6, 0, 4, HttpHeaderBody.POST, 0, 4);
            var isPut = Arrays.equals(first6, 0, 3, HttpHeaderBody.PUT, 0, 3);
            var isDelete = Arrays.equals(first6, 0, 6, HttpHeaderBody.DELETE, 0, 6);
            isHttp = isGet || isPost || isPut || isDelete;
            isRepl = Arrays.equals(first6, 0, 6, Repl.PROTOCOL_KEYWORD_BYTES, 0, 6);

            // set reader index back
            buf.readerIndex(0);

            // Parse request based on the protocol type
            if (isHttp) {
                var h = new HttpHeaderBody();
                h.feed(buf, buf.readableBytes(), 0);
                if (!h.isFullyRead()) {
                    return null;
                }
                httpHeaders = h.headers();

                if (isGet || isDelete) {
                    // Parse query parameters from the URL
                    var pos = h.url.indexOf("?");
                    if (pos == -1) {
                        data = new byte[1][];
                        data[0] = "zzz".getBytes(StandardCharsets.UTF_8);
                    } else {
                        var arr = h.url.substring(pos + 1).split("&");
                        data = new byte[arr.length][];
                        for (int i = 0; i < arr.length; i++) {
                            data[i] = URLDecoder.decode(arr[i], StandardCharsets.UTF_8)
                                    .getBytes(StandardCharsets.UTF_8);
                        }
                    }
                } else {
                    var body = h.body();
                    if (body == null) {
                        data = new byte[1][];
                        data[0] = "zzz".getBytes(StandardCharsets.UTF_8);
                    } else {
                        var arr = new String(body, StandardCharsets.UTF_8).split(" ");
                        data = new byte[arr.length][];
                        for (int i = 0; i < arr.length; i++) {
                            data[i] = arr[i].getBytes(StandardCharsets.UTF_8);
                        }
                    }
                }
            } else if (isRepl) {
                replRequest = Repl.decode(buf);
                if (replRequest == null) {
                    return null;
                }
                // slave send repl request is always small
//                assert replRequest.isFullyRead();
                // wait for next time
                if (!replRequest.isFullyRead()) {
                    log.warn("Repl request not fully read, left to read={}, repl type={}", replRequest.leftToRead(), replRequest.getType());
                    return null;
                }
                data = null;
            } else {
                // Fallback to RESP decoding
                data = resp.decode(buf);
                if (data == null) {
                    return null;
                }

                // Not all data is available yet; wait for more buffers
                for (var bb : data) {
                    if (bb == null) {
                        return null;
                    }
                }
            }
        }

        // Skip the bytes those were consumed during decoding
        int consumedN = buf.readerIndex();
        bufs.skip(consumedN);

        if (isRepl) {
            var r = new Request(null, false, true);
            r.setReplRequest(replRequest);
            return r;
        }

        var r = new Request(data, isHttp, isRepl);
        r.setHttpHeaders(httpHeaders);

        if (resp.bigStringNoMemoryCopy.length != 0) {
            r.bigStringNoMemoryCopy = resp.bigStringNoMemoryCopy.copy();
            resp.bigStringNoMemoryCopy.reset();
        }

        return r;
    }

    /**
     * @param bufs the source of bytes to decode
     * @return a list of decoded Request objects, or {@code null} when no complete request
     * could be decoded yet (signals {@code BinaryChannelSupplier} to wait for more bytes).
     * Returning an empty list instead of {@code null} would make the event loop busy-loop,
     * which is why large values that span multiple TCP reads must return {@code null} here.
     */
    @Override
    public @Nullable ArrayList<Request> tryDecode(ByteBufs bufs) throws MalformedDataException {
        try {
            ArrayList<Request> pipeline = new ArrayList<>();
            var one = tryDecodeOne(bufs);
            while (one != null) {
                pipeline.add(one);
                one = tryDecodeOne(bufs);
            }
            return pipeline.isEmpty() ? null : pipeline;
        } catch (Exception e) {
            throw new MalformedDataException(e);
        }
    }
}
