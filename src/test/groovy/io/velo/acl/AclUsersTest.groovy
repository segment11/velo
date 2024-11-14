package io.velo.acl

import spock.lang.Specification

class AclUsersTest extends Specification {
    def 'test all'() {
        given:
        def aclUsers = AclUsers.getInstance()
        // for coverage
        long[] testThreadIds = [0]
        aclUsers.initByNetWorkerThreadIds(testThreadIds)
        // for unit test
        aclUsers.initForTest()

        when:
        aclUsers.upInsert('test') { u ->
            u.password = U.Password.plain('123456')
        }
        then:
        aclUsers.get('test').password != null

        when:
        aclUsers.upInsert('test') { u ->
            u.password = U.Password.NO_PASSWORD
        }
        then:
        aclUsers.get('test').password.isNoPass()

        when:
        aclUsers.delete('test')
        then:
        aclUsers.get('test') == null
    }
}
