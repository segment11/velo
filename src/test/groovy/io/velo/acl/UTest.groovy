package io.velo.acl

import io.velo.BaseCommand
import spock.lang.Specification

class UTest extends Specification {
    def 'test base'() {
        given:
        def u = new U('kerry')
        u.password = U.Password.plain('123456')
        def p2 = U.Password.sha256('123456')

        and:
        def rCmd = new RCmd()
        rCmd.allow = true
        rCmd.type = RCmd.Type.all
        u.rCmdList << rCmd

        def rCmd2 = new RCmd()
        rCmd2.allow = false
        rCmd2.type = RCmd.Type.cmd
        rCmd2.cmd = 'set'
        u.rCmdDisallowList << rCmd2

        def rKey = new RKey()
        rKey.type = RKey.Type.read
        rKey.pattern = 'a*'
        u.rKeyList << rKey

        def rPubSub = new RPubSub()
        rPubSub.pattern = 'myChannel*'
        u.rPubSubList << rPubSub

        expect:
        u.on
        !u.password.isNoPass()
        u.password.check('123456')
        !u.password.check('1234567')
        u.literal() == 'user kerry on 123456 +* -set %R~a* &myChannel*'

        when:
        u.on = false
        then:
        u.literal() == 'user kerry off 123456 +* -set %R~a* &myChannel*'

        when:
        u.password = U.Password.sha256('123456')
        then:
        u.password.check('123456')

        when:
        u.on = true
        u.password = U.Password.NO_PASSWORD
        then:
        u.password.isNoPass()
        u.password.check('123456')
        u.literal() == 'user kerry on nopass +* -set %R~a* &myChannel*'

        when:
        def u1 = U.fromLiteral('user kerry on nopass +@all -@dangerous %R~a* ~b* &myChannel*')
        then:
        u1.user == 'kerry'
        u1.rCmdList.size() == 1
        u1.rCmdDisallowList.size() == 1
        u1.rKeyList.size() == 2
        u1.rPubSubList.size() == 1

        when:
        def u2 = U.fromLiteral('user kerry off 123456 ~*')
        then:
        u2.user == 'kerry'

        when:
        def u3 = U.fromLiteral('user kerry off')
        then:
        u3 == null

        when:
        def u4 = U.fromLiteral('kerry on')
        then:
        u4 == null

        when:
        boolean exception = false
        try {
            U.fromLiteral('user kerry on nopass !@all')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        u.addRCmd(true, new RCmd())
        u.addRCmdDisallow(true, new RCmd())
        u.addRKey(true, new RKey())
        u.addRPubSub(true, new RPubSub())
        then:
        u.rCmdList.size() == 1
        u.rCmdDisallowList.size() == 1
        u.rKeyList.size() == 1
        u.rPubSubList.size() == 1

        when:
        u.addRCmd(false, new RCmd())
        u.addRCmdDisallow(false, new RCmd())
        u.addRKey(false, new RKey())
        u.addRPubSub(false, new RPubSub())
        then:
        u.rCmdList.size() == 2
        u.rCmdDisallowList.size() == 2
        u.rKeyList.size() == 2
        u.rPubSubList.size() == 2

        when:
        def uRefer = new U('refer')
        u.mergeRulesFromAnother(uRefer, false)
        u.mergeRulesFromAnother(uRefer, true)
        then:
        1 == 1
    }

    def 'test check'() {
        given:
        def u = new U('kerry')
        def dataGet = new byte[2][]
        dataGet[0] = 'get'.bytes
        dataGet[1] = 'key'.bytes

        expect:
        !u.checkCmdAndKey('get', dataGet, null)

        when:
        def rCmd1 = new RCmd()
        rCmd1.allow = true
        rCmd1.type = RCmd.Type.cmd
        rCmd1.cmd = 'get'
        u.rCmdList << rCmd1
        then:
        u.checkCmdAndKey('get', dataGet, null)

        when:
        def dataMget = new byte[3][]
        dataMget[0] = 'mget'.bytes
        dataMget[1] = 'key1'.bytes
        dataMget[2] = 'key2'.bytes
        then:
        !u.checkCmdAndKey('mget', dataMget, null)

        when:
        def rCmd2 = new RCmd()
        rCmd2.allow = true
        rCmd2.type = RCmd.Type.cmd
        rCmd2.cmd = 'set'
        u.rCmdList << rCmd2
        def rCmd22 = new RCmd()
        rCmd22.allow = false
        rCmd22.type = RCmd.Type.cmd_with_first_arg
        rCmd22.cmd = 'set'
        rCmd22.firstArg = 'special_key'
        u.rCmdDisallowList << rCmd22
        def dataSet = new byte[3][]
        dataSet[0] = 'set'.bytes
        dataSet[1] = 'key'.bytes
        dataSet[2] = 'value'.bytes
        then:
        u.checkCmdAndKey('set', dataSet, null)

        when:
        dataSet[1] = 'special_key'.bytes
        then:
        !u.checkCmdAndKey('set', dataSet, null)

        when:
        u.rCmdDisallowList.clear()
        def rKey = new RKey()
        rKey.type = RKey.Type.read
        rKey.pattern = 'k*'
        u.rKeyList << rKey
        dataGet[1] = 'key'.bytes
        def slotWithKeyHashList = [BaseCommand.slot('key'.bytes, (short) 1)]
        then:
        u.checkCmdAndKey('get', dataGet, slotWithKeyHashList)

        when:
        dataGet[1] = 'other_key'.bytes
        def slotWithKeyHashList2 = [BaseCommand.slot('other_key'.bytes, (short) 1)]
        then:
        !u.checkCmdAndKey('get', dataGet, slotWithKeyHashList2)
        !u.checkChannels('my_channel1')

        when:
        def rPubSub = new RPubSub()
        rPubSub.pattern = 'my_channel*'
        u.rPubSubList << rPubSub
        then:
        u.checkChannels('my_channel1')
        !u.checkChannels('your_channel1')
    }
}
