package io.velo.decode;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import org.jetbrains.annotations.VisibleForTesting;

import java.net.URLDecoder;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

/**
 * Represents an HTTP header and body parser.
 * This class is responsible for parsing HTTP request data, extracting headers,
 * and handling the request body based on the content length.
 */
public class HttpHeaderBody {
    static final byte[] GET = "GET".getBytes();
    static final byte[] POST = "POST".getBytes();
    static final byte[] PUT = "PUT".getBytes();
    static final byte[] DELETE = "DELETE".getBytes();

    private static final int HEADER_BUFFER_LENGTH = 4096;
    private static final String HEADER_CONTENT_LENGTH = "Content-Length";

    /**
     * The HTTP 200 OK header prefix.
     */
    public static final byte[] HEADER_PREFIX_200 = "HTTP/1.1 200 OK\r\nCache-Control: no-cache, no-store\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes();
    /**
     * The HTTP 404 Not Found header prefix.
     */
    public static final byte[] HEADER_PREFIX_404 = "HTTP/1.1 404 Not Found\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes();
    /**
     * The default body for a 404 response.
     */
    public static final byte[] BODY_404 = "404: ".getBytes();
    /**
     * The HTTP 500 Internal Server Error header prefix.
     */
    public static final byte[] HEADER_PREFIX_500 = "HTTP/1.1 500 Internal Server Error\r\nCache-Control: no-cache, no-store\r\nContent-Type: text/plain\r\nContent-Length: ".getBytes();
    /**
     * The HTTP 401 Unauthorized header.
     */
    public static final byte[] HEADER_401 = "HTTP/1.1 401 Unauthorized\r\nWWW-Authenticate: Basic realm=\"Access to the staging site\"\r\nContent-Type: text/plain\r\nContent-Length: 0\r\n\r\n".getBytes();
    /**
     * The suffix for HTTP headers.
     */
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

    /**
     * Parses the HTTP action of the request.
     *
     * @return the action (e.g., "GET", "POST")
     */
    public String action() {
        return action;
    }

    /**
     * Parses the HTTP request type (method).
     *
     * @return the request type (e.g., "GET", "POST")
     */
    public String requestType() {
        return requestType;
    }

    /**
     * Parses the HTTP version.
     *
     * @return the HTTP version (e.g., "HTTP/1.1")
     */
    public String httpVersion() {
        return httpVersion;
    }

    /**
     * Retrieves the URL from the HTTP request.
     *
     * @return the URL of the request
     */
    public String url() {
        return url;
    }

    /**
     * Checks if the HTTP request is correctly parsed.
     *
     * @return true if the request is correctly parsed, false otherwise
     */
    public boolean isFullyRead() {
        return isFullyRead;
    }

    /**
     * Retrieves all headers from the HTTP request.
     *
     * @return the map of headers and their values
     */
    public Map<String, String> headers() {
        return headers;
    }

    /**
     * Retrieves the value of a specific header.
     *
     * @param name the name of the header
     * @return the value of the header, or null if the header is not found
     */
    public String header(String name) {
        return headers.get(name);
    }

    /**
     * Retrieves the content length from the HTTP request headers.
     *
     * @return the content length of the request body
     */
    public int contentLength() {
        if (contentLengthCache == -1) {
            var s = headers.get(HEADER_CONTENT_LENGTH);
            contentLengthCache = s != null ? Integer.parseInt(s.trim()) : 0;
        }
        return contentLengthCache;
    }

    /**
     * Retrieves the body of the HTTP request.
     *
     * @return the body of the request as a byte array
     */
    public byte[] body() {
        return body;
    }

    /**
     * Feeds byte data into the parser.
     *
     * @param data the byte data to feed
     */
    public void feed(byte[] data) {
        feed(data, data.length, 0);
    }

    /**
     * Feeds a portion of byte data into the parser.
     *
     * @param data   the byte data to feed
     * @param count  the number of bytes to feed
     * @param offset the offset in the byte array to start feeding from
     */
    public void feed(byte[] data, int count, int offset) {
        feed(Unpooled.wrappedBuffer(data), count, offset);
    }

    /**
     * Feeds a portion of byte data from a ByteBuf into the parser.
     *
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

            if (bf[headerLength - 1] == n && bf[headerLength - 2] == r) {
                if (action == null) {
                    action = new String(bf, startIndex, headerLength - startIndex - 2);
                    // parse action
                    var arr = action.split(" ");
                    if (arr.length != 3) {
                        isFullyRead = false;
                        return;
                    }

                    requestType = arr[0];
                    url = URLDecoder.decode(arr[1], StandardCharsets.UTF_8);
                    httpVersion = arr[2];
                    startIndex = headerLength;
                } else {
                    if (bf[headerLength - 3] == n && bf[headerLength - 4] == r) {
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

        isFullyRead = false;
    }
}