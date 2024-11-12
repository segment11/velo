package io.velo.acl

import org.apache.commons.codec.digest.DigestUtils
import spock.lang.Specification

class UTest extends Specification {
    def 'test all'() {
        given:
        def u = new U('kerry')
        u.password = new U.Password('123456', U.PasswordEncodedType.plain)

        and:
        def rCmd = new RCmd()
        rCmd.allow = true
        rCmd.type = RCmd.Type.all
        u.rCmdList << rCmd

        def rKey = new RKey()
        rKey.type = RKey.Type.read
        rKey.pattern = 'a*'
        u.rKeyList << rKey

        def rPubSub = new RPubSub()
        rPubSub.pattern = 'myChannel*'
        u.rPubSubList << rPubSub

        expect:
        !u.password.isNoPass()
        u.password.check('123456')
        !u.password.check('1234567')
        u.literal() == 'user kerry on >123456 +* %R~a* &myChannel*'

        when:
        u.isOn = false
        then:
        u.literal() == 'user kerry off >123456 +* %R~a* &myChannel*'

        when:
        u.password = new U.Password(new String(DigestUtils.sha256('123456')), U.PasswordEncodedType.sha256)
        then:
        u.password.check('123456')

        when:
        u.isOn = true
        u.password = new U.Password(U.NO_PASS, U.PasswordEncodedType.plain)
        then:
        u.password.isNoPass()
        u.password.check('123456')
        u.literal() == 'user kerry on nopass +* %R~a* &myChannel*'
    }
}
