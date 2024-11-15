package io.velo.acl

import spock.lang.Specification

class RPubSubTest extends Specification {
    def 'test all'() {
        given:
        def one = new RPubSub()
        one.pattern = 'myChannel*'

        expect:
        one.literal() == '&myChannel*'
        one.match('myChannel1')

        when:
        one.pattern = '*'
        then:
        one.match('yourChannel1')
    }

    def 'test from literal'() {
        expect:
        RPubSub.isRPubSubLiteral('&myChannel*')
        RPubSub.isRPubSubLiteral('allchannels')
        !RPubSub.isRPubSubLiteral('_allchannels')
        RPubSub.fromLiteral('allchannels').pattern == '*'
        RPubSub.fromLiteral('&myChannel*').pattern == 'myChannel*'

        when:
        boolean exception = false
        try {
            RPubSub.fromLiteral('myChannel*')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
