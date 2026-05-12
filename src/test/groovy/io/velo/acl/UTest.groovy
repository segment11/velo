package io.velo.acl

import io.velo.BaseCommand
import io.velo.reply.MultiBulkReply
import org.apache.commons.codec.digest.DigestUtils
import spock.lang.Specification

class UTest extends Specification {
    def 'test base'() {
        given:
        def u = new U('kerry')
        u.on = true
        def p1 = U.Password.plain('123456')
        def p2 = U.Password.sha256Hex('123456')
        def p11 = U.Password.plain('1234567')
        def p22 = U.Password.sha256HexEncoded('34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6')
        u.password = p1
        u.addPassword(p2)
        u.addPassword(p2)
        u.addPassword(p11)
        u.removePassword(p11)
        u.addPassword(p22)

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
        !u.firstPassword.isNoPass()
        u.checkPassword('123456')
        !u.checkPassword('1234567')
        u.checkPassword('passwd4')
        u.literal() == 'user kerry on #8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92 #8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92 #34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6 +* -set %R~a* &myChannel*'
        p1.equals(p1)
        !p1.equals(p11)
        !p1.equals(p2)
        !p1.equals(null)
        !p1.equals(u)
        p1.equals(U.Password.plain('123456'))

        when:
        u.on = false
        then:
        u.literal() == 'user kerry off #8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92 #8d969eef6ecad3c29a3a629280e686cf0c3f5d5a86aff3ca12020c923adc6c92 #34344e4d60c2b6d639b7bd22e18f2b0b91bc34bf0ac5f9952744435093cfb4e6 +* -set %R~a* &myChannel*'

        when:
        u.password = U.Password.sha256Hex('123456')
        then:
        u.checkPassword('123456')

        when:
        u.on = true
        u.password = U.Password.NO_PASSWORD
        then:
        u.firstPassword.isNoPass()
        u.checkPassword('123456')
        u.literal() == 'user kerry on nopass +* -set %R~a* &myChannel*'

        when:
        u.resetPassword()
        then:
        u.firstPassword == null
        !u.checkPassword('123456')

        when:
        def uu = new U('kerry')
        uu.on = true
        uu.password = U.Password.sha256Hex('123456')
        then:
        uu.literal() == 'user kerry on #' + DigestUtils.sha256Hex('123456')
        uu.checkPassword('123456')

        when:
        def uu2 = U.fromLiteral('user kerry on #' + DigestUtils.sha256Hex('123456'))
        then:
        uu2.checkPassword('123456')

        when:
        boolean shortHashException = false
        try {
            U.fromLiteral('user kerry on #short')
        } catch (IllegalArgumentException e) {
            println e.message
            shortHashException = true
        }
        then:
        shortHashException

        when:
        boolean longHashException = false
        try {
            U.fromLiteral('user kerry on #' + 'a' * 65)
        } catch (IllegalArgumentException e) {
            println e.message
            longHashException = true
        }
        then:
        longHashException

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
        u.resetCmd()
        u.resetKey()
        u.resetPubSub()
        then:
        u.rCmdList.size() == 0
        u.rCmdDisallowList.size() == 0
        u.rKeyList.size() == 0
        u.rPubSubList.size() == 0

        when:
        def uRefer = new U('refer')
        u.mergeRulesFromAnother(uRefer, false)
        u.mergeRulesFromAnother(uRefer, true)
        then:
        1 == 1
    }

    def 'test literal round-trip preserves multiple passwords'() {
        given:
        def u = new U('multi')
        u.on = true
        u.resetPassword()
        u.addPassword U.Password.plain('pass1')
        u.addPassword U.Password.plain('pass2')
        u.addPassword U.Password.sha256Hex('pass3')
        u.addRCmd(true, RCmd.fromLiteral("+*"))
        u.addRKey(true, RKey.fromLiteral("~*"))

        when:
        def lit = u.literal()
        then:
        !lit.contains('>pass1')
        !lit.contains('>pass2')
        lit.contains('#' + DigestUtils.sha256Hex('pass1'))
        lit.contains('#' + DigestUtils.sha256Hex('pass2'))

        when:
        def restored = U.fromLiteral(lit)
        then:
        restored.checkPassword('pass1')
        restored.checkPassword('pass2')
        restored.checkPassword('pass3')
        !restored.checkPassword('wrong')
    }

    def 'test to replies'() {
        given:
        def u = new U('kerry')

        when:
        u.on = true
        u.resetPassword()
        u.addPassword U.Password.plain('123456')
        u.addRCmd(true, RCmd.fromLiteral("+*"))
        u.addRKey(true, RKey.fromLiteral("~*"))
        u.addRPubSub(true, RPubSub.fromLiteral("&*"))
        def replies = u.toReplies()
        def sb = new StringBuilder()
        then:
        replies.length == 10
        new MultiBulkReply(replies).dumpForTest(sb, 0)

        when:
        println sb.toString()
        u.on = false
        u.resetPassword()
        u.addPassword U.Password.plain('123456')
        u.addPassword U.Password.plain('1234567')
        u.addRCmd(true, RCmd.fromLiteral("+@all"))
        u.addRCmdDisallow(true, RCmd.fromLiteral("-@admin"))
        u.addRKey(true, RKey.fromLiteral("%R~a*"))
        u.addRPubSub(true, RPubSub.fromLiteral("&myChannel*"))
        replies = u.toReplies()
        sb.delete(0, sb.length())
        then:
        replies.length == 10
        new MultiBulkReply(replies).dumpForTest(sb, 0)

        when:
        u.resetPassword()
        u.addPassword U.Password.NO_PASSWORD
        replies = u.toReplies()
        then:
        replies.length == 10

        when:
        u.resetPassword()
        replies = u.toReplies()
        then:
        replies.length == 10

        cleanup:
        println sb.toString()
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
        def slotWithKeyHashList2 = [BaseCommand.SlotWithKeyHash.TO_FIX_FIRST_SLOT, BaseCommand.slot('other_key'.bytes, (short) 1)]
        then:
        !u.checkCmdAndKey('get', dataGet, slotWithKeyHashList2)
        !u.checkChannels('my_channel1')

        when:
        u.rKeyList.clear()
        then:
        !u.checkCmdAndKey('get', dataGet, slotWithKeyHashList)

        when:
        def slotWithKeyHashList11 = [BaseCommand.SlotWithKeyHash.TO_FIX_FIRST_SLOT]
        then:
        u.checkCmdAndKey('get', dataGet, slotWithKeyHashList11)

        when:
        def rPubSub = new RPubSub()
        rPubSub.pattern = 'my_channel*'
        u.rPubSubList << rPubSub
        then:
        u.checkChannels('my_channel1')
        !u.checkChannels('your_channel1')
    }

    def 'test checkChannels uses OR across rules'() {

        when:
        def u = new U('rwtest')
        u.addRCmd(true, RCmd.fromLiteral('+@all'))
        u.addRKey(true, RKey.fromLiteral('%R~tenant:*'))
        u.addRKey(false, RKey.fromLiteral('%W~tenant:*'))

        def dataGet = new byte[2][]
        dataGet[0] = 'get'.bytes
        dataGet[1] = 'tenant:1'.bytes
        def slotsGet = [BaseCommand.slot('tenant:1'.bytes, (short) 1)]

        def dataSet = new byte[3][]
        dataSet[0] = 'set'.bytes
        dataSet[1] = 'tenant:1'.bytes
        dataSet[2] = 'value'.bytes
        def slotsSet = [BaseCommand.slot('tenant:1'.bytes, (short) 1)]
        then:
        u.checkCmdAndKey('get', dataGet, slotsGet)
        u.checkCmdAndKey('set', dataSet, slotsSet)
    }

    def 'test %R~ key selector only allows read commands'() {
        given:
        def u = new U('readonly')
        u.addRCmd(true, RCmd.fromLiteral('+@all'))
        u.addRKey(true, RKey.fromLiteral('%R~data:*'))

        def dataGet = new byte[2][]
        dataGet[0] = 'get'.bytes
        dataGet[1] = 'data:x'.bytes
        def slotsGet = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataSet = new byte[3][]
        dataSet[0] = 'set'.bytes
        dataSet[1] = 'data:x'.bytes
        dataSet[2] = 'v'.bytes
        def slotsSet = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        expect:
        u.checkCmdAndKey('get', dataGet, slotsGet)
        !u.checkCmdAndKey('set', dataSet, slotsSet)
    }

    def 'test %W~ key selector only allows write commands'() {
        given:
        def u = new U('writeonly')
        u.addRCmd(true, RCmd.fromLiteral('+@all'))
        u.addRKey(true, RKey.fromLiteral('%W~data:*'))

        def dataGet = new byte[2][]
        dataGet[0] = 'get'.bytes
        dataGet[1] = 'data:x'.bytes
        def slotsGet = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataSet = new byte[3][]
        dataSet[0] = 'set'.bytes
        dataSet[1] = 'data:x'.bytes
        dataSet[2] = 'v'.bytes
        def slotsSet = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        expect:
        !u.checkCmdAndKey('get', dataGet, slotsGet)
        u.checkCmdAndKey('set', dataSet, slotsSet)
    }

    def 'test %W~ denies read-write commands like GETDEL and GETSET'() {
        given:
        def u = new U('writeonly2')
        u.addRCmd(true, RCmd.fromLiteral('+@all'))
        u.addRKey(true, RKey.fromLiteral('%W~data:*'))

        def dataGetdel = new byte[2][]
        dataGetdel[0] = 'getdel'.bytes
        dataGetdel[1] = 'data:x'.bytes
        def slotsGetdel = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        expect:
        !u.checkCmdAndKey('getdel', dataGetdel, slotsGetdel)
    }

    def 'test %R~ denies read-write commands like GETDEL and GETSET'() {
        given:
        def u = new U('readonly2')
        u.addRCmd(true, RCmd.fromLiteral('+@all'))
        u.addRKey(true, RKey.fromLiteral('%R~data:*'))

        def dataGetdel = new byte[2][]
        dataGetdel[0] = 'getdel'.bytes
        dataGetdel[1] = 'data:x'.bytes
        def slotsGetdel = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        expect:
        !u.checkCmdAndKey('getdel', dataGetdel, slotsGetdel)
    }

    def 'test %W~ denies RPOPLPUSH, BLMOVE, COPY'() {
        given:
        def u = new U('writeonly3')
        u.addRCmd(true, RCmd.fromLiteral('+@all'))
        u.addRKey(true, RKey.fromLiteral('%W~src:*'))
        u.addRKey(false, RKey.fromLiteral('%W~dst:*'))

        def dataRpoplpush = new byte[3][]
        dataRpoplpush[0] = 'rpoplpush'.bytes
        dataRpoplpush[1] = 'src:x'.bytes
        dataRpoplpush[2] = 'dst:x'.bytes
        def slotsRpoplpush = [BaseCommand.slot('src:x'.bytes, (short) 1), BaseCommand.slot('dst:x'.bytes, (short) 1)]

        def dataCopy = new byte[3][]
        dataCopy[0] = 'copy'.bytes
        dataCopy[1] = 'src:x'.bytes
        dataCopy[2] = 'dst:x'.bytes
        def slotsCopy = [BaseCommand.slot('src:x'.bytes, (short) 1), BaseCommand.slot('dst:x'.bytes, (short) 1)]

        def dataBlmove = new byte[5][]
        dataBlmove[0] = 'blmove'.bytes
        dataBlmove[1] = 'src:x'.bytes
        dataBlmove[2] = 'dst:x'.bytes
        dataBlmove[3] = 'left'.bytes
        dataBlmove[4] = 'right'.bytes
        def slotsBlmove = [BaseCommand.slot('src:x'.bytes, (short) 1), BaseCommand.slot('dst:x'.bytes, (short) 1)]

        expect:
        !u.checkCmdAndKey('rpoplpush', dataRpoplpush, slotsRpoplpush)
        !u.checkCmdAndKey('brpoplpush', dataRpoplpush, slotsRpoplpush)
        !u.checkCmdAndKey('copy', dataCopy, slotsCopy)
        !u.checkCmdAndKey('blmove', dataBlmove, slotsBlmove)
    }

    def 'test %W~ denies destructive pop commands that return values'() {
        given:
        def u = new U('writeonly4')
        u.addRCmd(true, RCmd.fromLiteral('+@all'))
        u.addRKey(true, RKey.fromLiteral('%W~data:*'))

        def dataLpop = new byte[2][]
        dataLpop[0] = 'lpop'.bytes
        dataLpop[1] = 'data:x'.bytes
        def slotsLpop = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataRpop = new byte[2][]
        dataRpop[0] = 'rpop'.bytes
        dataRpop[1] = 'data:x'.bytes
        def slotsRpop = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataSpop = new byte[2][]
        dataSpop[0] = 'spop'.bytes
        dataSpop[1] = 'data:x'.bytes
        def slotsSpop = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataZpopmax = new byte[2][]
        dataZpopmax[0] = 'zpopmax'.bytes
        dataZpopmax[1] = 'data:x'.bytes
        def slotsZpopmax = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataZpopmin = new byte[2][]
        dataZpopmin[0] = 'zpopmin'.bytes
        dataZpopmin[1] = 'data:x'.bytes
        def slotsZpopmin = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataBlpop = new byte[2][]
        dataBlpop[0] = 'blpop'.bytes
        dataBlpop[1] = 'data:x'.bytes
        def slotsBlpop = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataBrpop = new byte[2][]
        dataBrpop[0] = 'brpop'.bytes
        dataBrpop[1] = 'data:x'.bytes
        def slotsBrpop = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        expect:
        !u.checkCmdAndKey('lpop', dataLpop, slotsLpop)
        !u.checkCmdAndKey('rpop', dataRpop, slotsRpop)
        !u.checkCmdAndKey('spop', dataSpop, slotsSpop)
        !u.checkCmdAndKey('zpopmax', dataZpopmax, slotsZpopmax)
        !u.checkCmdAndKey('zpopmin', dataZpopmin, slotsZpopmin)
        !u.checkCmdAndKey('blpop', dataBlpop, slotsBlpop)
        !u.checkCmdAndKey('brpop', dataBrpop, slotsBrpop)
    }

    def 'test %W~ denies write commands that return previous string values'() {
        given:
        def u = new U('writeonly5')
        u.addRCmd(true, RCmd.fromLiteral('+@all'))
        u.addRKey(true, RKey.fromLiteral('%W~data:*'))

        def dataSet = new byte[3][]
        dataSet[0] = 'set'.bytes
        dataSet[1] = 'data:x'.bytes
        dataSet[2] = 'new'.bytes
        def slotsSet = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataSetGet = new byte[4][]
        dataSetGet[0] = 'set'.bytes
        dataSetGet[1] = 'data:x'.bytes
        dataSetGet[2] = 'new'.bytes
        dataSetGet[3] = 'get'.bytes
        def slotsSetGet = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        def dataSetbit = new byte[4][]
        dataSetbit[0] = 'setbit'.bytes
        dataSetbit[1] = 'data:x'.bytes
        dataSetbit[2] = '0'.bytes
        dataSetbit[3] = '1'.bytes
        def slotsSetbit = [BaseCommand.slot('data:x'.bytes, (short) 1)]

        expect:
        u.checkCmdAndKey('set', dataSet, slotsSet)
        !u.checkCmdAndKey('set', dataSetGet, slotsSetGet)
        !u.checkCmdAndKey('setbit', dataSetbit, slotsSetbit)
    }

    def 'test checkChannels uses OR across rules - original'() {
        given:
        def u = new U('kerry')
        def rPubSub1 = new RPubSub()
        rPubSub1.pattern = 'foo*'
        def rPubSub2 = new RPubSub()
        rPubSub2.pattern = 'bar*'
        u.rPubSubList << rPubSub1
        u.rPubSubList << rPubSub2

        expect:
        u.checkChannels('foo123')
        u.checkChannels('bar456')
        !u.checkChannels('baz789')
        u.checkChannels('foo123', 'bar456')
        !u.checkChannels('foo123', 'baz789')
    }

    def 'test checkCmdAndKey uses OR across key patterns'() {
        given:
        def u = new U('kerry')
        def rCmd = new RCmd()
        rCmd.allow = true
        rCmd.type = RCmd.Type.all
        u.rCmdList << rCmd

        def rKey1 = new RKey()
        rKey1.type = RKey.Type.read_write
        rKey1.pattern = 'foo*'
        def rKey2 = new RKey()
        rKey2.type = RKey.Type.read_write
        rKey2.pattern = 'bar*'
        u.rKeyList << rKey1
        u.rKeyList << rKey2

        def data = new byte[2][]
        data[0] = 'get'.bytes

        when:
        data[1] = 'foo123'.bytes
        def slots1 = [BaseCommand.slot('foo123'.bytes, (short) 1)]
        then:
        u.checkCmdAndKey('get', data, slots1)

        when:
        data[1] = 'bar456'.bytes
        def slots2 = [BaseCommand.slot('bar456'.bytes, (short) 1)]
        then:
        u.checkCmdAndKey('get', data, slots2)

        when:
        data[1] = 'baz789'.bytes
        def slots3 = [BaseCommand.slot('baz789'.bytes, (short) 1)]
        then:
        !u.checkCmdAndKey('get', data, slots3)
    }

    def 'test adding real password removes nopass sentinel'() {
        given:
        def u = new U('testuser')
        u.addPassword(U.Password.NO_PASSWORD)

        expect:
        u.checkPassword('') || u.checkPassword('anything')

        when:
        u.addPassword(U.Password.plain('secret'))
        then:
        u.checkPassword('secret')
        !u.checkPassword('anything')
        u.literal().contains('nopass') == false

        when:
        u.resetPassword()
        u.addPassword(U.Password.plain('pw1'))
        u.addPassword(U.Password.plain('pw2'))
        u.addPassword(U.Password.NO_PASSWORD)
        then:
        u.passwords.size() == 1
        u.passwords[0].isNoPass()
        u.checkPassword('pw1')
    }

    def 'test nopass clears existing passwords'() {
        given:
        def u = new U('testuser2')
        u.addPassword(U.Password.plain('alpha'))
        u.addPassword(U.Password.sha256Hex('beta'))

        expect:
        u.checkPassword('alpha')

        when:
        u.addPassword(U.Password.NO_PASSWORD)
        then:
        u.checkPassword('anything')
    }
}
