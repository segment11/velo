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
        one.match('acl', null)
        !one.match('acl_x', null)

        when:
        one.allow = false
        then:
        one.literal() == '-acl'
        one.match('acl', null)
        !one.match('acl_x', null)

        when:
        one.allow = true
        one.type = RCmd.Type.cmd_with_first_arg
        one.cmd = 'acl'
        one.firstArg = 'cat'
        then:
        one.literal() == '+acl|cat'
        one.match('acl', 'cat')
        !one.match('acl_x', 'cat')
        !one.match('acl', 'cat_x')
        !one.match('acl_x', 'cat_x')
        !one.match('acl', null)

        when:
        one.allow = false
        then:
        one.literal() == '-acl|cat'
        one.match('acl', 'cat')

        when:
        one.allow = true
        one.type = RCmd.Type.category
        one.category = Category.admin
        then:
        one.literal() == '+@admin'
        one.match('acl', null)
        one.match('acl_x', null)
        !one.match('bitcount', null)

        when:
        one.allow = false
        then:
        one.literal() == '-@admin'
        one.match('acl', null)

        when:
        one.allow = true
        one.category = Category.all
        then:
        one.literal() == '+@all'
        one.match('acl', null)

        when:
        one.allow = true
        one.type = RCmd.Type.all
        then:
        one.literal() == '+*'
        one.match('acl', null)

        when:
        one.allow = false
        then:
        one.literal() == '-*'
        one.match('acl', null)
    }

    def 'test from literal'() {
        expect:
        RCmd.isRCmdLiteral('+acl')
        RCmd.isRCmdLiteral('-acl')
        RCmd.isRCmdLiteral('allcommands')
        RCmd.isRCmdLiteral('nocommands')
        !RCmd.isRCmdLiteral('_nocommands')
        RCmd.isAllowLiteral('+acl')
        !RCmd.isAllowLiteral('-acl')
        RCmd.isAllowLiteral('allcommands')
        !RCmd.isAllowLiteral('nocommands')
        RCmd.fromLiteral("allcommands").type == RCmd.Type.category
        RCmd.fromLiteral("nocommands").type == RCmd.Type.category

        when:
        def one = RCmd.fromLiteral('+acl')
        def two = RCmd.fromLiteral('-acl')
        def three = RCmd.fromLiteral('+acl|cat')
        def four = RCmd.fromLiteral('-acl|cat')
        def five = RCmd.fromLiteral('+@admin')
        def six = RCmd.fromLiteral('-@admin')
        def seven = RCmd.fromLiteral('+*')
        def eight = RCmd.fromLiteral('-*')
        then:
        one.allow
        one.type == RCmd.Type.cmd
        one.cmd == 'acl'
        one.firstArg == null
        !two.allow
        two.type == RCmd.Type.cmd
        two.cmd == 'acl'
        two.firstArg == null
        three.allow
        three.type == RCmd.Type.cmd_with_first_arg
        three.cmd == 'acl'
        three.firstArg == 'cat'
        !four.allow
        four.type == RCmd.Type.cmd_with_first_arg
        four.cmd == 'acl'
        four.firstArg == 'cat'
        five.allow
        five.type == RCmd.Type.category
        five.category == Category.admin
        !six.allow
        six.type == RCmd.Type.category
        six.category == Category.admin
        seven.allow
        seven.type == RCmd.Type.all
        !eight.allow
        eight.type == RCmd.Type.all

        when:
        boolean exception = false
        try {
            RCmd.fromLiteral('xxx')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }
}
