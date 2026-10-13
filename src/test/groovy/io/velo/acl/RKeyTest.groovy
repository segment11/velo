package io.velo.acl

import spock.lang.Specification

class RKeyTest extends Specification {
    def 'test all'() {
        given:
        def one = new RKey()
        one.type = RKey.Type.all

        expect:
        one.literal() == '~*'
        one.match('a', true, true)

        when:
        one.type = RKey.Type.read
        one.pattern = 'a*'
        then:
        one.literal() == '%R~a*'
        one.match('a1', true, false)
        !one.match('a1', false, true)
        !one.match('b1', true, false)

        when:
        one.type = RKey.Type.write
        then:
        one.literal() == '%W~a*'
        one.match('a1', false, true)
        !one.match('a1', true, false)
        !one.match('b1', false, true)

        when:
        one.type = RKey.Type.read_write
        then:
        one.literal() == '%RW~a*'
        one.match('a1', true, false)
        one.match('a1', false, true)
    }

    def 'test glob pattern'() {
        given:
        // acl key patterns use full glob semantics, see bug 70.4
        def one = new RKey()
        one.type = RKey.Type.read_write

        when:
        one.pattern = 'user:*:cart'
        then:
        one.match('user:1:cart', true, false)
        !one.match('user:1:wishlist', true, false)

        when:
        one.pattern = 'h?llo'
        then:
        one.match('hello', true, false)
        one.match('hallo', true, false)
        !one.match('hxello', true, false)

        when:
        one.pattern = 'cache:[a-c]*'
        then:
        one.match('cache:b1', true, false)
        !one.match('cache:d1', true, false)
    }
}

    def 'test from literal'() {
        expect:
        RKey.isRKeyLiteral('~*')
        RKey.isRKeyLiteral('%R~a*')
        RKey.isRKeyLiteral('%W~a*')
        RKey.isRKeyLiteral('%RW~a*')
        RKey.isRKeyLiteral('allkeys')
        !RKey.isRKeyLiteral('_allkeys')
        RKey.fromLiteral('allkeys').type == RKey.Type.all
        RKey.fromLiteral('~*').type == RKey.Type.all
        RKey.fromLiteral('%R~a*').type == RKey.Type.read
        RKey.fromLiteral('%W~a*').type == RKey.Type.write
        RKey.fromLiteral('%RW~a*').type == RKey.Type.read_write
        RKey.fromLiteral('~a*').type == RKey.Type.read_write

        when:
        boolean exception = false
        try {
            RKey.fromLiteral('a*')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            RKey.fromLiteral('test~')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
