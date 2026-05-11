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

    def 'test upInsert does not double-apply callback on current thread inner'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()

        def callCount = 0
        when:
        aclUsers.upInsert('counter-user') { u ->
            callCount++
            u.password = U.Password.plain('pw')
        }
        then:
        callCount == 1
        aclUsers.get('counter-user') != null

        when:
        callCount = 0
        aclUsers.delete('counter-user')
        then:
        aclUsers.get('counter-user') == null

        cleanup:
        aclUsers.initForTest()
    }

    def 'test replaceUsers does not double-apply on current thread inner'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()

        aclUsers.upInsert('to-replace') { u ->
            u.password = U.Password.plain('old')
        }

        // Replace with a single user that has a known password
        List<U> newUsers = [new U('replaced-user')]
        newUsers[0].password = U.Password.plain('new-pw')

        when:
        aclUsers.replaceUsers(newUsers)
        def user = aclUsers.get('replaced-user')
        then:
        user != null
        user.checkPassword('new-pw')
        aclUsers.get('to-replace') == null

        cleanup:
        aclUsers.initForTest()
    }

    def 'test upInsert before init does not NPE and state preserved after init'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.resetForTest()

        when:
        aclUsers.upInsert(U.DEFAULT_USER) { u ->
            u.password = U.Password.plain('pre-init-password')
        }
        then:
        noExceptionThrown()

        when:
        aclUsers.initForTest()
        def defaultUser = aclUsers.get(U.DEFAULT_USER)
        then:
        defaultUser != null
        defaultUser.checkPassword('pre-init-password')
    }

    def 'test upInsert before initBySlotWorkerEventloopArray preserves state'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.resetForTest()

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

        when:
        aclUsers.upInsert(U.DEFAULT_USER) { u ->
            u.password = U.Password.plain('staged-password')
        }
        Eventloop[] testEventloopArray = [eventloop, eventloopCurrent]
        aclUsers.initBySlotWorkerEventloopArray(testEventloopArray)
        Thread.sleep(200)
        def defaultUser = aclUsers.get(U.DEFAULT_USER)
        then:
        defaultUser != null
        defaultUser.checkPassword('staged-password')
        eventloop.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get(U.DEFAULT_USER) != null
        })).get()

        cleanup:
        eventloop.breakEventloop()
    }
}
