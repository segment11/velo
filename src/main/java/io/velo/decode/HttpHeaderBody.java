package io.velo.decode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.HashMap;
import java.util.Map;

/**
 * HTTP header and body parser for incoming requests.
 */
public class HttpHeaderBody {
    static final byte[] GET = "GET".getBytes();
    static final byte[] POST = "POST".getBytes();
    static final byte[] PUT = "PUT".getBytes();
    static final byte[] DELETE = "DELETE".getBytes();

    private static final int HEADER_BUFFER_LENGTH = 4096;
    private static final String HEADER_CONTENT_LENGTH = "Content-Length";

    /** HTTP 200 OK response header prefix. */
    public static final byte[] HEADER_PREFIX_200 = "HTTP/1.1 200 OK\r\nCache-Control: no-cache, no-store\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes();
    /** HTTP 404 Not Found response header prefix. */
    public static final byte[] HEADER_PREFIX_404 = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes();
    /** Default body content for 404 responses. */
    public static final byte[] BODY_404 = "404: ".getBytes();
    /** HTTP 500 Internal Server Error response header prefix. */
    public static final byte[] HEADER_PREFIX_500 = "HTTP/1.1 500 Internal Server Error\r\nCache-Control: no-cache, no-store\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes();
    /** HTTP 401 Unauthorized response header. */
    public static final byte[] HEADER_401 = "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"Access to the staging site\"\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n".getBytes();
    /** HTTP header suffix (CRLF CRLF). */
    public static final byte[] HEADER_SUFFIX = "\r\n\r\n".getBytes();

    private static final byte r = '\r';
    private static final byte n = '\n';
    private static final byte e = ':';

    private final byte[] headerBuffer = new byte[HEADER_BUFFER_LENGTH];

    private int headerLength = 0;
    private int startIndex = 0;

    @VisibleForTesting
    String action;

    @VisibleForTesting
    String requestType;

    @VisibleForTesting
    String httpVersion;

    @VisibleForTesting
    String url;

    private boolean isFullyRead = false;

    private String lastHeaderName;

    private final Map<String, String> headers = new HashMap<>();

    private int contentLengthCache = -1;

    private byte[] body;

    /** The parsed HTTP action (e.g., "GET", "POST"). */
    public String action() {
        return action;
    }

    /** The parsed HTTP request method (e.g., "GET", "POST"). */
    public String requestType() {
        return requestType;
    }

    /** The parsed HTTP version (e.g., "HTTP/1.1"). */
    public String httpVersion() {
        return httpVersion;
    }

    /** The parsed URL from the HTTP request. */
    public String url() {
        return url;
    }

    /** True if the request has been fully parsed. */
    public boolean isFullyRead() {
        return isFullyRead;
    }

    /** All HTTP headers from the request. */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * @param name the header name
     * @return the header value, or null if not found
     */
    public String header(String name) {
        var exact = headers.get(name);
        if (exact != null) {
            return exact;
        }

        for (var entry : headers.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(name)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * @return the content length from the request headers
     */
    public int contentLength() {
        if (contentLengthCache == -1) {
            var s = header(HEADER_CONTENT_LENGTH);
            if (s != null && !s.trim().isEmpty()) {
                try {
                    contentLengthCache = Integer.parseInt(s.trim());
                    if (contentLengthCache < 0) {
                        throw new IllegalArgumentException("Http content length should be non-negative");
                    }
                } catch (NumberFormatException e) {
                    throw new IllegalArgumentException("Http content length is invalid", e);
                }
            } else {
                contentLengthCache = 0;
            }
        }
        return contentLengthCache;
    }

    /** The request body as a byte array. */
    public byte[] body() {
        return body;
    }

    /**
     * @param data the byte data to feed
     */
    public void feed(byte[] data) {
        feed(data, data.length, 0);
    }

    /**
     * @param data   the byte data to feed
     * @param count  the number of bytes to feed
     * @param offset the offset in the byte array to start feeding from
     */
    public void feed(byte[] data, int count, int offset) {
        feed(Unpooled.wrappedBuffer(data), count, offset);
    }

    /**
     * @param nettyBuf the ByteBuf containing the byte data
     * @param count    the number of bytes to feed
     * @param offset   the offset in the ByteBuf to start feeding from
     */
    public void feed(ByteBuf nettyBuf, int count, int offset) {
        if (count > HEADER_BUFFER_LENGTH) {
            throw new IllegalArgumentException("Http header too long");
        }

        var bf = headerBuffer;
        while (count > 0) {
            bf[headerLength] = nettyBuf.getByte(offset);

            headerLength++;
            offset++;
            count--;

            if (bf[headerLength - 1] == n) {
                if (headerLength < 2 || bf[headerLength - 2] != r) {
                    throw new IllegalArgumentException("Malformed http line ending");
                }
                if (action == null) {
                    action = new String(bf, startIndex, headerLength - startIndex - 2);
                    // parse action
                    var arr = action.split(" ");
                    if (arr.length != 3) {
                        throw new IllegalArgumentException("Malformed http request line: " + action);
                    }

                    requestType = arr[0];
                    url = arr[1];
                    httpVersion = arr[2];
                    startIndex = headerLength;
                } else {
                    if (headerLength >= 4 && bf[headerLength - 3] == n && bf[headerLength - 4] == r) {
                        var contentLength = contentLength();
                        var totalLength = headerLength + contentLength;
                        if (totalLength <= nettyBuf.readableBytes()) {
                            isFullyRead = true;
                            nettyBuf.readerIndex(nettyBuf.readerIndex() + headerLength);

                            if (contentLength > 0) {
                                body = new byte[contentLength];
                                nettyBuf.readBytes(body);
                            }
                        }
                        return;
                    } else {
                        if (lastHeaderName != null) {
                            var headerName = lastHeaderName;
                            if (!headerName.equals(headerName.trim())) {
                                throw new IllegalArgumentException("Malformed http header name");
                            }
                            if (headerName.chars().anyMatch(Character::isWhitespace)) {
                                throw new IllegalArgumentException("Malformed http header name");
                            }
                            if (headerName.isEmpty()) {
                                throw new IllegalArgumentException("Malformed http header name");
                            }
                            headers.put(headerName, new String(bf, startIndex, headerLength - startIndex - 2).trim());
                            startIndex = headerLength;
                            lastHeaderName = null;
                        } else {
                            throw new IllegalArgumentException("Malformed http header line");
                        }
                    }
                }
            } else if (bf[headerLength - 1] == e && lastHeaderName == null) {
                if (headerLength - startIndex - 1 <= 0) {
                    throw new IllegalArgumentException("Malformed http header name");
                }
                lastHeaderName = new String(bf, startIndex, headerLength - startIndex - 1);
                startIndex = headerLength;
            }
        }

        isFullyRead = false;
    }
}
