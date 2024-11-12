package io.velo.acl

import spock.lang.Specification

class RPubSubTest extends Specification {
    def 'test all'() {
        given:
        def one = new RPubSub()
        one.pattern = 'myChannel*'

        expect:
        one.literal() == '&myChannel*'
        one.check('myChannel1')

        when:
        one.pattern = '*'
        then:
        one.check('yourChannel1')
    }
}
