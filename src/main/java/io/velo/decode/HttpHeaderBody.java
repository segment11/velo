package io.velo.decode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.VisibleForTesting;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

public class HttpHeaderBody {
    static final byte[] GET = "GET".getBytes();
    static final byte[] POST = "POST".getBytes();
    static final byte[] PUT = "PUT".getBytes();
    static final byte[] DELETE = "DELETE".getBytes();

    private static final int HEADER_BUFFER_LENGTH = 4096;
    private static final String HEADER_CONTENT_LENGTH = "Content-Length";

    public static final byte[] HEADER_PREFIX_200 = "HTTP/1.1 200 OK\r\nCache-Control: no-cache, no-store\r\nContent-Length: ".getBytes();
    public static final byte[] HEADER_PREFIX_404 = "HTTP/1.1 404 Not Found\r\nContent-Length: ".getBytes();
    public static final byte[] BODY_404 = "404: ".getBytes();
    public static final byte[] HEADER_PREFIX_500 = "HTTP/1.1 500 Internal Server Error\r\nCache-Control: no-cache, no-store\r\nContent-Length: ".getBytes();
    public static final byte[] HEADER_401 = "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"Access to the staging site\"\r\nContent-Length: 0\r\n\r\n".getBytes();
    public static final byte[] HEADER_SUFFIX = "\r\n\r\n".getBytes();

    private static final byte r = '\r';
    private static final byte n = '\n';
    private static final byte e = ':';

    private final byte[] headerBuffer = new byte[HEADER_BUFFER_LENGTH];

    private int headerLength = 0;
    private int startIndex = 0;

    public String action() {
        return action;
    }

    public String requestType() {
        return requestType;
    }

    public String httpVersion() {
        return httpVersion;
    }

    public String url() {
        return url;
    }

    public boolean isOk() {
        return isOk;
    }

    @VisibleForTesting
    String action;
    @VisibleForTesting
    String requestType;
    @VisibleForTesting
    String httpVersion;
    @VisibleForTesting
    String url;
    @VisibleForTesting
    boolean isOk = false;

    private String lastHeaderName;

    private Map<String, String> headers = new HashMap<>();

    public Map<String, String> getHeaders() {
        return headers;
    }

    public String header(String name) {
        return headers.get(name);
    }

    private int contentLengthCache = -1;

    public int contentLength() {
        if (contentLengthCache == -1) {
            var s = headers.get(HEADER_CONTENT_LENGTH);
            contentLengthCache = s != null ? Integer.parseInt(s.trim()) : 0;
        }
        return contentLengthCache;
    }

    private byte[] body;

    public byte[] body() {
        return body;
    }

    public void feed(byte[] data) {
        feed(data, data.length, 0);
    }

    public void feed(byte[] data, int count, int offset) {
        feed(Unpooled.wrappedBuffer(data), count, offset);
    }

    public void feed(ByteBuf buf, int count, int offset) {
        if (count > HEADER_BUFFER_LENGTH) {
            throw new IllegalArgumentException("Http header too long");
        }

        var bf = headerBuffer;
        while (count > 0) {
            bf[headerLength] = buf.getByte(offset);

            headerLength++;
            offset++;
            count--;

            if (bf[headerLength - 1] == n && bf[headerLength - 2] == r) {
                if (action == null) {
                    action = new String(bf, startIndex, headerLength - startIndex - 2);
                    // parse action
                    var arr = action.split(" ");
                    if (arr.length != 3) {
                        isOk = false;
                        return;
                    }

                    requestType = arr[0];
                    url = URLDecoder.decode(arr[1], StandardCharsets.UTF_8);
                    httpVersion = arr[2];
                    startIndex = headerLength;
                } else {
                    if (bf[headerLength - 3] == n && bf[headerLength - 4] == r) {
//                        if (lastHeaderName != null) {
//                            headers.put(lastHeaderName, new String(bf, startIndex, headerLength - startIndex - 2));
//                        }

                        var contentLength = contentLength();
                        var totalLength = headerLength + contentLength;
                        if (totalLength <= buf.readableBytes()) {
                            isOk = true;
                            buf.readerIndex(buf.readerIndex() + headerLength);

                            if (contentLength > 0) {
                                body = new byte[contentLength];
                                buf.readBytes(body);
                            }
                        }
                        return;
                    } else {
                        if (lastHeaderName != null) {
                            headers.put(lastHeaderName.trim(), new String(bf, startIndex, headerLength - startIndex - 2).trim());
                            startIndex = headerLength;
                            lastHeaderName = null;
                        }
                    }
                }
            } else if (bf[headerLength - 1] == e && lastHeaderName == null) {
                lastHeaderName = new String(bf, startIndex, headerLength - startIndex - 1);
                startIndex = headerLength;
            }
        }

        isOk = false;
    }
}
