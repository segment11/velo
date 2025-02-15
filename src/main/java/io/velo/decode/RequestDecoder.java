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

public class RequestDecoder implements ByteBufsDecoder<ArrayList<Request>> {
    // in local thread
    private final RESP resp = new RESP();

    private Request tryDecodeOne(ByteBufs bufs) throws MalformedDataException {
        var compositeByteBuf = Unpooled.compositeBuffer();
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

            if (isHttp) {
                var h = new HttpHeaderBody();
                h.feed(compositeByteBuf, compositeByteBuf.readableBytes(), 0);
                if (!h.isOk) {
                    return null;
                }
                httpHeaders = h.getHeaders();

                if (isGet || isDelete) {
                    // query parameters
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
                data = resp.decode(compositeByteBuf);
                if (data == null) {
                    return null;
                }

                // not parse all, need wait next buffer from client
                for (var bb : data) {
                    if (bb == null) {
                        return null;
                    }
                }
            }
        }

        // remove already consumed bytes
        int consumedN = compositeByteBuf.readerIndex();
        bufs.takeExactSize(consumedN);

        var r = new Request(data, isHttp, isRepl);
        r.setHttpHeaders(httpHeaders);
        return r;
    }

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
