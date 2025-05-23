package io.velo.decode;

import io.activej.bytebuf.ByteBufs;
import io.activej.common.exception.MalformedDataException;
import io.activej.csp.binary.decoder.ByteBufsDecoder;
import io.netty.buffer.Unpooled;
import io.velo.repl.Repl;
import org.jetbrains.annotations.Nullable;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Map;

/**
 * A decoder for incoming requests that can handle both HTTP and RESP (Redis Serialization Protocol) protocols.
 * The decoder reads from a {@link ByteBufs} input and attempts to parse as many complete requests as possible.
 */
public class RequestDecoder implements ByteBufsDecoder<ArrayList<Request>> {
    // in local thread
    private final RESP resp = new RESP();

    /**
     * Attempts to decode a single request from the provided ByteBufs.
     * The method checks for HTTP keywords at the beginning of the buffer, and if not found, treats the input as RESP.
     *
     * @param bufs the source of bytes to decode
     * @return a decoded {@link Request} or null if there aren't enough bytes to form a complete request
     * @throws MalformedDataException if the data in ByteBufs is malformed and cannot be decoded
     */
    private Request tryDecodeOne(ByteBufs bufs) throws MalformedDataException {
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

        // check first 6 bytes, http or repl at least 6 bytes
        byte[][] data;
        boolean isHttp = false;
        boolean isRepl = false;
        Map<String, String> httpHeaders = null;
        if (capacity < 6) {
            data = resp.decode(compositeByteBuf);
            if (data == null) {
                return null;
            }
        } else {
            var first6 = new byte[6];
            compositeByteBuf.readBytes(first6);
            var isGet = Arrays.equals(first6, 0, 3, HttpHeaderBody.GET, 0, 3);
            var isPost = Arrays.equals(first6, 0, 4, HttpHeaderBody.POST, 0, 4);
            var isPut = Arrays.equals(first6, 0, 3, HttpHeaderBody.PUT, 0, 3);
            var isDelete = Arrays.equals(first6, 0, 6, HttpHeaderBody.DELETE, 0, 6);
            isHttp = isGet || isPost || isPut || isDelete;
            isRepl = Arrays.equals(first6, 0, 6, Repl.PROTOCOL_KEYWORD_BYTES, 0, 6);

            // set reader index back
            compositeByteBuf.readerIndex(0);

            // Parse request based on the protocol type
            if (isHttp) {
                var h = new HttpHeaderBody();
                h.feed(compositeByteBuf, compositeByteBuf.readableBytes(), 0);
                if (!h.isOk) {
                    return null;
                }
                httpHeaders = h.getHeaders();

                if (isGet || isDelete) {
                    // Parse query parameters from the URL
                    var pos = h.url.indexOf("?");
                    if (pos == -1) {
                        return null;
                    }

                    var arr = h.url.substring(pos + 1).split("&");
                    data = new byte[arr.length][];
                    for (int i = 0; i < arr.length; i++) {
                        data[i] = arr[i].getBytes();
                    }
                } else {
                    var body = h.body();
                    if (body == null) {
                        return null;
                    }

                    var arr = new String(body).split(" ");
                    data = new byte[arr.length][];
                    for (int i = 0; i < arr.length; i++) {
                        data[i] = arr[i].getBytes();
                    }
                }
            } else if (isRepl) {
                data = Repl.decode(compositeByteBuf);
                if (data == null) {
                    return null;
                }
            } else {
                // Fallback to RESP decoding
                data = resp.decode(compositeByteBuf);
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
        int consumedN = compositeByteBuf.readerIndex();
        bufs.skip(consumedN);

        var r = new Request(data, isHttp, isRepl);
        r.setHttpHeaders(httpHeaders);
        return r;
    }

    /**
     * Attempts to decode multiple requests from the provided ByteBufs until no more complete requests can be formed.
     *
     * @param bufs the source of bytes to decode
     * @return a list of decoded {@link Request} objects or null if no complete requests were found
     * @throws MalformedDataException if the data in ByteBufs is malformed and cannot be decoded
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