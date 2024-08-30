package io.velo.decode


import spock.lang.Specification

class HttpHeaderBodyTest extends Specification {
    def "test feed"() {
        given:
        def h = new HttpHeaderBody()
        def h2 = new HttpHeaderBody()
        def h3 = new HttpHeaderBody()

        when:
        def getHeader = "GET / HTTP/1.1\r\nAccept: text/html\r\n\r\n".bytes
        h.feed(getHeader)
        then:
        h.isOk
        h.isOk()
        h.action == "GET / HTTP/1.1"
        h.action() == "GET / HTTP/1.1"
        h.httpVersion == "HTTP/1.1"
        h.httpVersion() == "HTTP/1.1"
        h.requestType == "GET"
        h.requestType() == "GET"
        h.url == "/"
        h.url() == "/"
        h.contentLength() == 0
        h.headers.size() == 1
        h.header('Accept')

        when:
        def b1 = "GET /?1=1 HTTP/1.1\r\nX-Key: X-value\r\n".bytes
        def h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        !h1.isOk

        when:
        b1 = "GET /?1=1 HTTP/1.1\r\nAccept: text:/html\r\n\r\n".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        h1.isOk

        when:
        b1 = "POST /?1=1 HTTP/1.1\r\nAccept: text/html\r\nX-key: X-value\r\nContent-Length: 4\r\n\r\n1234".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        h1.isOk

        when:
        b1 = "GET /?1=1 HTTP/1.1\r\nAccept: text/html\r\n".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        !h1.isOk

        when:
        b1 = "GET /?1=1\r".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        !h1.isOk

        when:
        b1 = "GET /?1=1\n\n".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        !h1.isOk

        when:
        b1 = "GET /?1=1\r\nAccept: text/htm\r\n".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        !h1.isOk

        when:
        b1 = "GET /?1=1 HTTP/1.1\r\nAccept: text/html\r\r\r\n".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        !h1.isOk

        when:
        b1 = "GET /?1=1 HTTP/1.1\r\nAccept: text/html\n\n\r\n".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        !h1.isOk


        when:
        b1 = "GET /?1=1 HTTP/1.1\r\nAccept: text/htmlxx\r\n".bytes
        h1 = new HttpHeaderBody()
        h1.feed(b1)
        then:
        !h1.isOk

        when:
        def postHeader = "POST / HTTP/1.1\r\nAccept: text/html\r\nX-key: X-value\r\nContent-Length: 4\r\n\r\n".bytes
        h2.feed(postHeader)
        then:
        !h2.isOk
        h2.action == "POST / HTTP/1.1"
        h2.httpVersion == "HTTP/1.1"
        h2.requestType == "POST"
        h2.url == "/"
        h2.contentLength() == 4
        h2.headers.size() == 3

        when:
        def fullHttpHeaderWithBody = "POST / HTTP/1.1\r\nContent-Length: 4\r\n\r\n1234".bytes
        h3.feed(fullHttpHeaderWithBody)
        then:
        h3.isOk
        h3.action == "POST / HTTP/1.1"
        h3.httpVersion == "HTTP/1.1"
        h3.requestType == "POST"
        h3.url == "/"
        h3.contentLength() == 4
        h3.headers.size() == 1
        '1234' == new String(h3.body())

        when:
        def getHeader2 = new byte[4097]
        def execption = false
        try {
            h.feed(getHeader2)
        } catch (IllegalArgumentException e) {
            println e.message
            execption = true
        }
        then:
        execption
    }
}
