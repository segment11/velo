package io.velo.acl

import io.activej.async.callback.AsyncComputation
import io.activej.common.function.SupplierEx
import io.activej.eventloop.Eventloop
import io.velo.ValkeyRawConfSupport
import spock.lang.Specification

import java.nio.file.Paths
import java.time.Duration

class AclUsersTest extends Specification {
    def 'test all'() {
        given:
        def aclUsers = AclUsers.getInstance()

        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }
        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        Thread.sleep(100)
        Eventloop[] testEventloopArray = [eventloop, eventloopCurrent]
        aclUsers.initBySlotWorkerEventloopArray(testEventloopArray)
        aclUsers.upInsert('test') { u ->
            u.password = U.Password.plain('123456')
        }
        Thread.sleep(1000)
        expect:
        eventloop.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get('test') != null
        })).get()

        when:
        aclUsers.initForTest()
        aclUsers.upInsert('test') { u ->
            u.password = U.Password.plain('123456')
        }
        then:
        aclUsers.get('test').firstPassword != null
        aclUsers.inner.users.size() > 0

        when:
        aclUsers.upInsert('test') { u ->
            u.password = U.Password.NO_PASSWORD
        }
        then:
        aclUsers.get('test').firstPassword.isNoPass()

        when:
        aclUsers.delete('test')
        then:
        aclUsers.get('test') == null

        when:
        List<U> users = []
        users << new U('test1')
        aclUsers.replaceUsers(users)
        then:
        aclUsers.get('test1') != null

        when:
        def aclFile = Paths.get(ValkeyRawConfSupport.aclFilename).toFile()
        if (!aclFile.exists()) {
            aclFile.createNewFile()
        }
        aclFile.text = 'user default on nopass ~* +@all &*\r\n# comment'
        aclUsers.loadAclFile()
        then:
        aclUsers.get('default') != null

        when:
        boolean exception = false
        // no default user
        aclFile.text = 'user test on nopass ~* +@all &*'
        try {
            aclUsers.loadAclFile()
        } catch (Exception e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        // not valid user literal
        aclFile.text = '_user default on nopass ~* +@all &*'
        try {
            aclUsers.loadAclFile()
        } catch (Exception e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        aclFile.delete()
        aclUsers.loadAclFile()
        then:
        1 == 1

        cleanup:
        eventloop.breakEventloop()
    }
}
