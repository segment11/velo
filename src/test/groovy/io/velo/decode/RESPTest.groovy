package io.velo.decode

import io.netty.buffer.Unpooled
import spock.lang.Specification

class RESPTest extends Specification {
    def 'test decode normal'() {
        given:
        def resp = new RESP()

        when:
        def getCmdStr = '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n'
        def setCmdStr = '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n'
        def cmdStr = '+0\r\n'
        def getCmdData = resp.decode(Unpooled.wrappedBuffer(getCmdStr.bytes))
        def setCmdData = resp.decode(Unpooled.wrappedBuffer(setCmdStr.bytes))
        def cmdData = resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        then:
        getCmdData.length == 2
        getCmdData[0] == 'GET'.bytes
        getCmdData[1] == 'mykey'.bytes
        setCmdData.length == 3
        setCmdData[0] == 'SET'.bytes
        setCmdData[1] == 'mykey'.bytes
        setCmdData[2] == 'myvalue'.bytes
        cmdData.length == 0
    }

    def 'test decode not normal'() {
        given:
        def resp = new RESP()

        when:
        def cmdStr = '*2'
        def cmdData = resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        then:
        cmdData == null

        when:
        cmdStr = '*2\rx'
        cmdData = resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        then:
        cmdData == null

        when:
        cmdStr = '*x\r\n'
        boolean exception = false
        try {
            resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cmdStr = '*12345678901234567890\r\n'
        exception = false
        try {
            resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cmdStr = '*\r\n'
        exception = false
        try {
            resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cmdStr = '*a\r\n'
        exception = false
        try {
            resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cmdStr = '*.\r\n'
        exception = false
        try {
            resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cmdStr = '*-\r\n'
        exception = false
        try {
            resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cmdStr = '*-1\r\n'
        exception = false
        try {
            resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        } catch (NegativeArraySizeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        cmdStr = '*2\r\n$3\r\nGET\r\n$5\r\n'
        cmdData = resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        then:
        cmdData != null
        cmdData[0] == 'GET'.bytes
        cmdData[1] == null

        when:
        cmdStr = '*2\r\n$3\r\nGET\r\n'
        cmdData = resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        then:
        cmdData != null
        cmdData[0] == 'GET'.bytes
        cmdData[1] == null

        when:
        cmdStr = '*2\r\n$3\r\nGET\r\n$'
        cmdData = resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        then:
        cmdData != null
        cmdData[0] == 'GET'.bytes
        cmdData[1] == null

        when:
        cmdStr = '*2\r\n$3\r\nGET\r\nx'
        exception = false
        try {
            cmdData = resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        cmdData != null
        cmdData[0] == 'GET'.bytes
        cmdData[1] == null
        exception

        when:
        cmdStr = '*2\r\n$3\r\nGET\r\n$5'
        cmdData = resp.decode(Unpooled.wrappedBuffer(cmdStr.bytes))
        then:
        cmdData != null
        cmdData[0] == 'GET'.bytes
        cmdData[1] == null

        when:
        cmdData = resp.decode(Unpooled.wrappedBuffer(new byte[0]))
        then:
        cmdData == null

        when:
        exception = false
        try {
            resp.decode(Unpooled.wrappedBuffer(new byte[1]))
        } catch (Exception e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
