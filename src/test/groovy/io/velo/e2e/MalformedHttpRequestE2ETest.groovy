package io.velo.e2e

import io.velo.test.tools.VeloServer
import spock.lang.Specification

import java.net.Socket
import java.net.SocketTimeoutException
import java.nio.charset.StandardCharsets

class MalformedHttpRequestE2ETest extends Specification {
    def 'test malformed complete http request line does not hang server'() {
        given:
        if (!VeloServer.isJarExists()) {
            println 'velo jar not built, skip'
            expect:
            true
            return
        }

        def suffix = System.nanoTime()
        def serverDir = new File("/tmp/velo-e2e-malformed-http-${suffix}")
        def server = new VeloServer('malformed-http-request')
                .randomPort()
                .dir(serverDir.absolutePath)
        def serverThread = Thread.start {
            server.run()
        }

        and:
        assert TestUtil.waitUntil(30, 1_000L) {
            try {
                def jedis = server.jedis()
                try {
                    return jedis.ping() == 'PONG'
                } finally {
                    jedis.close()
                }
            } catch (Exception ignored) {
                return false
            }
        }

        when:
        def rawRequest = "GET /bad\r\nHost: localhost:${server.port}\r\n\r\n"
        def result = sendRawHttp(server.port, rawRequest)

        then:
        result.response != 'SOCKET_TIMEOUT'
        result.firstRead == -1 || !result.response.contains('HTTP/1.1 200 OK')

        when:
        def regularHeaderMalformedPost = """POST /debug
Host: localhost:${server.port}
User-Agent: curl/8.7.1
Accept: */*
Content-Type: text/plain
Content-Length: 9

get mykey""".replace('\n', '\r\n')
        def postResult = sendRawHttp(server.port, regularHeaderMalformedPost)

        then:
        postResult.response != 'SOCKET_TIMEOUT'
        postResult.firstRead == -1 || !postResult.response.contains('HTTP/1.1 200 OK')

        cleanup:
        server?.stop()
        serverThread?.join(5_000)
        new File("velo-port${server?.port}.properties").delete()
        serverDir?.deleteDir()
    }

    private static Map<String, Object> sendRawHttp(int port, String rawRequest) {
        int firstRead = Integer.MIN_VALUE
        String response = null
        Socket client = null
        try {
            client = new Socket('127.0.0.1', port)
            client.soTimeout = 2_000
            println "Sending raw HTTP bytes to ${port}:"
            println rawRequest.replace('\r', '\\r').replace('\n', '\\n\n')
            client.outputStream.write(rawRequest.getBytes(StandardCharsets.UTF_8))
            client.outputStream.flush()

            def readBuffer = new byte[256]
            firstRead = client.inputStream.read(readBuffer)
            if (firstRead > 0) {
                response = new String(readBuffer, 0, firstRead, StandardCharsets.UTF_8)
                println "First socket read bytes=${firstRead}"
                println "Response payload:"
                println response.replace('\r', '\\r').replace('\n', '\\n\n')
            } else {
                println "First socket read bytes=${firstRead}"
            }
        } catch (SocketTimeoutException e) {
            response = 'SOCKET_TIMEOUT'
            println "Socket timed out waiting for malformed HTTP response"
        } finally {
            client?.close()
        }
        [firstRead: firstRead, response: response]
    }
}
