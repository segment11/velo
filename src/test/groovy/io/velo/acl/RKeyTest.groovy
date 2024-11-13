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

    def 'test from literal'() {
        expect:
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
