package io.velo.repl.incremental

import io.velo.acl.AclUsers
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.BinlogContent
import io.velo.repl.ReplPairTest
import spock.lang.Specification

import java.nio.ByteBuffer

class XAclUpdateTest extends Specification {
    def 'test encode and decode'() {
        given:
        def xAclUpdate = new XAclUpdate("acl save")

        expect:
        xAclUpdate.type() == BinlogContent.Type.acl_update

        when:
        def encoded = xAclUpdate.encodeWithType()
        def buffer = ByteBuffer.wrap(encoded)
        buffer.get()
        def xAclUpdate1 = xAclUpdate.decodeFrom(buffer)
        then:
        xAclUpdate1.encodedLength() == encoded.length

        when:
        boolean exception = false
        buffer.putInt(1, 0)
        buffer.position(1)
        try {
            xAclUpdate.decodeFrom(buffer)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        AclUsers.instance.initForTest()
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def replPair = ReplPairTest.mockAsSlave()
        xAclUpdate.apply(slot, replPair)
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
