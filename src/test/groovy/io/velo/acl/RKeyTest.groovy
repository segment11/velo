package io.velo.acl

import spock.lang.Specification

class RKeyTest extends Specification {
    def 'test all'() {
        given:
        def one = new RKey()
        one.type = RKey.Type.all

        expect:
        one.literal() == '~*'
        one.check('a', true, true)

        when:
        one.type = RKey.Type.read
        one.pattern = 'a*'
        then:
        one.literal() == '%R~a*'
        one.check('a1', true, false)
        !one.check('a1', false, true)
        !one.check('b1', true, false)

        when:
        one.type = RKey.Type.write
        then:
        one.literal() == '%W~a*'
        one.check('a1', false, true)
        !one.check('a1', true, false)
        !one.check('b1', false, true)

        when:
        one.type = RKey.Type.read_write
        then:
        one.literal() == '%RW~a*'
        one.check('a1', true, false)
        one.check('a1', false, true)
    }
}
