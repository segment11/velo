package io.velo.decode

import io.activej.bytebuf.ByteBuf
import io.activej.bytebuf.ByteBufs
import spock.lang.Specification

class ReuseBufsTest extends Specification {
    def 'test decode resp'() {
        given:
        def reuseBufs = new ReuseBufs()

        and:
        def buf1 = ByteBuf.wrapForReading(
                ('*3\r\n$4\r\nMGET\r\n$5\r\nmykey\r\n$6\r\nmykey1\r\n' +
                        '*3\r\n$3\r\nSET\r\n$5\r\nmykey\r\n$7\r\nmyvalue\r\n').bytes
        )
        def buf2 = ByteBuf.wrapForReading(
                '*2\r\n$3\r\nGET\r\n$5\r\nmykey\r\n'.bytes
        )

        when:
        def bufs1 = new ByteBufs()
        bufs1.add(buf1)
        bufs1.add(buf2)
        reuseBufs.decodeFromBufs(bufs1)
        reuseBufs.printForDebug()
        then:
        reuseBufs.isFeedFull()

        when:
        def bufs2 = new ByteBufs()
        bufs2.add(buf2)
        bufs2.add(buf1)
        reuseBufs.decodeFromBufs(bufs2)
        reuseBufs.printForDebug()
        then:
        reuseBufs.isFeedFull()

        when:
        def bufs3 = new ByteBufs()
        bufs3.add(buf2)
        reuseBufs.decodeFromBufs(bufs3)
        reuseBufs.printForDebug()
        then:
        reuseBufs.isFeedFull()

        when:
        def bufs4 = new ByteBufs()
        bufs4.add(buf1)
        reuseBufs.decodeFromBufs(bufs4)
        reuseBufs.printForDebug()
        then:
        reuseBufs.isFeedFull()
    }

    def 'test decode resp fail'() {
        given:
        def reuseBufs = new ReuseBufs()

        when:
        def buf = ByteBuf.wrapForReading(
                '*'.bytes
        )
        def bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        buf = ByteBuf.wrapForReading(
                '+0\r\n'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        buf = ByteBuf.wrapForReading(
                '*3\r\n'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        buf = ByteBuf.wrapForReading(
                '*3\r\n$4'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        buf = ByteBuf.wrapForReading(
                '*44444'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        buf = ByteBuf.wrapForReading(
                '*3\r\n$44444'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        buf = ByteBuf.wrapForReading(
                '*3\r\n$4\r'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        buf = ByteBuf.wrapForReading(
                '*3\r\n$4\r\nMGETxy'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        buf = ByteBuf.wrapForReading(
                new byte[0]
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        reuseBufs.decodeFromBufs(bufs)
        then:
        !reuseBufs.isFeedFull()

        when:
        boolean exception = false
        buf = ByteBuf.wrapForReading(
                '?'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        try {
            reuseBufs.decodeFromBufs(bufs)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buf = ByteBuf.wrapForReading(
                '*3\r\n*4'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        try {
            reuseBufs.decodeFromBufs(bufs)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buf = ByteBuf.wrapForReading(
                '*12345678901\r\n'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        try {
            reuseBufs.decodeFromBufs(bufs)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buf = ByteBuf.wrapForReading(
                '*a\r\n'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        try {
            reuseBufs.decodeFromBufs(bufs)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buf = ByteBuf.wrapForReading(
                '*3\r\n$12345678901\r\n'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        try {
            reuseBufs.decodeFromBufs(bufs)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        buf = ByteBuf.wrapForReading(
                '*3\r\n$a\r\n'.bytes
        )
        bufs = new ByteBufs(1)
        bufs.add(buf)
        try {
            reuseBufs.decodeFromBufs(bufs)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
