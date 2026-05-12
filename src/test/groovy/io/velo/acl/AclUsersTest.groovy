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

    def 'test write methods route through eventloop for non-owner thread'() {
        given:
        def aclUsers = AclUsers.instance

        // Two eventloops on separate threads
        def eventloop1 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop1.keepAlive(true)
        Thread.start {
            eventloop1.run()
        }
        def eventloop2 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop2.keepAlive(true)
        Thread.start {
            eventloop2.run()
        }
        Thread.sleep(200)
        Eventloop[] testEventloopArray = [eventloop1, eventloop2]
        aclUsers.initBySlotWorkerEventloopArray(testEventloopArray)

        // Call upInsert from a non-worker thread (test thread)
        when:
        aclUsers.upInsert('routed-user') { u ->
            u.password = U.Password.plain('routed-pw')
        }
        Thread.sleep(500)
        // Verify both eventloop threads got the user
        def userOnEl1 = eventloop1.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get('routed-user')
        })).get()
        def userOnEl2 = eventloop2.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get('routed-user')
        })).get()
        then:
        userOnEl1 != null
        userOnEl1.checkPassword('routed-pw')
        userOnEl2 != null
        userOnEl2.checkPassword('routed-pw')

        when:
        def deleteResult = aclUsers.delete('routed-user')
        Thread.sleep(500)
        def deletedOnEl1 = eventloop1.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get('routed-user')
        })).get()
        def deletedOnEl2 = eventloop2.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get('routed-user')
        })).get()
        then:
        deleteResult
        deletedOnEl1 == null
        deletedOnEl2 == null

        when:
        def deleteNonExistent = aclUsers.delete('nonexistent-user')
        then:
        !deleteNonExistent

        cleanup:
        eventloop1.breakEventloop()
        eventloop2.breakEventloop()
    }

    def 'test non-owner replaceUsers routes through eventloop'() {
        given:
        def aclUsers = AclUsers.instance

        def eventloop1 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop1.keepAlive(true)
        Thread.start {
            eventloop1.run()
        }
        def eventloop2 = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop2.keepAlive(true)
        Thread.start {
            eventloop2.run()
        }
        Thread.sleep(200)
        Eventloop[] testEventloopArray = [eventloop1, eventloop2]
        aclUsers.initBySlotWorkerEventloopArray(testEventloopArray)

        // First add a user
        aclUsers.upInsert('old-user') { u ->
            u.password = U.Password.plain('old-pw')
        }
        Thread.sleep(500)

        // Replace from non-owner thread
        List<U> newUsers = [new U('new-user')]
        newUsers[0].password = U.Password.plain('new-pw')

        when:
        aclUsers.replaceUsers(newUsers)
        Thread.sleep(500)
        def userOnEl1 = eventloop1.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get('new-user')
        })).get()
        def userOnEl2 = eventloop2.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get('new-user')
        })).get()
        def oldOnEl1 = eventloop1.submit(AsyncComputation.of(SupplierEx.of {
            aclUsers.get('old-user')
        })).get()
        then:
        userOnEl1 != null
        userOnEl1.checkPassword('new-pw')
        userOnEl2 != null
        userOnEl2.checkPassword('new-pw')
        oldOnEl1 == null

        cleanup:
        eventloop1.breakEventloop()
        eventloop2.breakEventloop()
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

    def 'test acl log records and retrieves entries'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()
        AclUsers.resetAclLogs()

        when:
        AclUsers.recordAclLog("command", "io-loop", "GET", "testuser", "/127.0.0.1:12345")
        AclUsers.recordAclLog("key", "io-loop", "SET", "admin", "/127.0.0.1:12346")

        then:
        def logs = AclUsers.getAclLogs(10)
        logs.length == 2
        logs[0].reason == "command"
        logs[0].object == "GET"
        logs[0].username == "testuser"
        logs[1].reason == "key"
        logs[1].object == "SET"
        logs[1].username == "admin"

        cleanup:
        AclUsers.resetAclLogs()
    }

    def 'test acl log respects count limit'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()
        AclUsers.resetAclLogs()

        when:
        AclUsers.recordAclLog("command", "io-loop", "GET", "user1", "/127.0.0.1:1")
        AclUsers.recordAclLog("command", "io-loop", "SET", "user2", "/127.0.0.1:2")
        AclUsers.recordAclLog("command", "io-loop", "DEL", "user3", "/127.0.0.1:3")

        then:
        def logs2 = AclUsers.getAclLogs(2)
        logs2.length == 2

        when:
        def logs1 = AclUsers.getAclLogs(1)
        then:
        logs1.length == 1

        cleanup:
        AclUsers.resetAclLogs()
    }

    def 'test acl log reset clears all entries'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()
        AclUsers.resetAclLogs()

        when:
        AclUsers.recordAclLog("command", "io-loop", "GET", "user", "/127.0.0.1:1")
        AclUsers.recordAclLog("key", "io-loop", "SET", "user", "/127.0.0.1:2")
        then:
        AclUsers.getAclLogs(10).length == 2

        when:
        AclUsers.resetAclLogs()
        then:
        AclUsers.getAclLogs(10).length == 0
    }

    def 'test acl log circular buffer behavior'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()
        AclUsers.resetAclLogs()

        when:
        for (int i = 0; i < 130; i++) {
            AclUsers.recordAclLog("command", "io-loop", "CMD" + i, "user" + i, "/127.0.0.1:" + i)
        }

        then:
        def logs = AclUsers.getAclLogs(128)
        logs.length == 128
        logs[0].object == "CMD2"
        logs[127].object == "CMD129"

        cleanup:
        AclUsers.resetAclLogs()
    }

    def 'test acl log getAclLogs returns empty when no logs'() {
        given:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()
        AclUsers.resetAclLogs()

        expect:
        AclUsers.getAclLogs(10).length == 0
        AclUsers.getAclLogs(0).length == 0
    }
}
