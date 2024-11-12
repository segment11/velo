package io.velo.acl

import spock.lang.Specification

class RCmdTest extends Specification {
    def 'test all'() {
        given:
        def one = new RCmd()
        one.allow = true
        one.type = RCmd.Type.cmd
        one.cmd = 'acl'

        expect:
        one.literal() == '+acl'
        one.check('acl', null)
        !one.check('acl_x', null)

        when:
        one.allow = false
        then:
        one.literal() == '-acl'
        !one.check('acl', null)
        !one.check('acl_x', null)

        when:
        one.allow = true
        one.type = RCmd.Type.cmd_with_first_arg
        one.cmd = 'acl'
        one.firstArg = 'cat'
        then:
        one.literal() == '+acl|cat'
        one.check('acl', 'cat')
        !one.check('acl_x', 'cat')
        !one.check('acl', 'cat_x')
        !one.check('acl_x', 'cat_x')

        when:
        one.allow = false
        then:
        one.literal() == '-acl|cat'
        !one.check('acl', 'cat')

        when:
        one.allow = true
        one.type = RCmd.Type.category
        one.category = 'admin'
        then:
        one.literal() == '+@admin'
        one.check('acl', null)
        one.check('acl_x', null)
        !one.check('bitcount', null)

        when:
        one.allow = false
        then:
        one.literal() == '-@admin'
        !one.check('acl', null)

        when:
        one.allow = true
        one.type = RCmd.Type.all
        then:
        one.literal() == '+*'
        one.check('acl', null)

        when:
        one.allow = false
        then:
        one.literal() == '-*'
        !one.check('acl', null)
    }
}
