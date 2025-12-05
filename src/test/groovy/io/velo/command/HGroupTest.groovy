package io.velo.command

import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.SocketInspectorTest
import io.velo.acl.AclUsers
import io.velo.acl.U
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisHH
import io.velo.type.RedisHashKeys
import spock.lang.Specification

class HGroupTest extends Specification {
    def _HGroup = new HGroup(null, null, null)

    static List<String> cmdLines = '''
hdel
hexists
hget
hgetall
hincrby
hincrbyfloat
hkeys
hlen
hmget
hmset
hscan
hrandfield
hset
hsetnx
hstrlen
hvals
hexpire
hexpireat
hexpiretime
hpersist
hpexpire
hpexpireat
hpexpiretime
hpttl
httl
'''.readLines().collect { it.trim() }.findAll { it }

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        expect:
        cmdLines.every {
            def sList = _HGroup.parseSlots(it, data2, slotNumber)
            sList.size() == 1
        }

        when:
        def sList = _HGroup.parseSlots('hxxx', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        def data1 = new byte[1][]
        then:
        cmdLines.every {
            def sList1 = _HGroup.parseSlots(it, data1, slotNumber)
            sList1.size() == 0
        }
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def hGroup = new HGroup('hdel', data1, null)
        hGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data2 = new byte[2][]
        data2[1] = '4'.bytes
        hGroup.cmd = 'hello'
        hGroup.data = data2
        reply = hGroup.handle()
        then:
        reply instanceof ErrorReply

        when:
        hGroup.data = data1
        def replyList = cmdLines.collect {
            hGroup.cmd = it
            hGroup.handle()
        }
        then:
        replyList.every { it == ErrorReply.FORMAT }

        when:
        hGroup.cmd = 'zzz'
        reply = hGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    final short slot = 0

    def 'test prefer use hh'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'field'.bytes

        def hGroup = new HGroup('hdel', data3, null)
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def keyBytes = 'a'.bytes
        then:
        !hGroup.isUseHH(keyBytes)

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        then:
        hGroup.isUseHH(keyBytes)

        when:
        keyBytes = (new String(RedisHH.PREFER_MEMBER_NOT_TOGETHER_KEY_PREFIX) + 'xxx').bytes
        then:
        !hGroup.isUseHH(keyBytes)

        when:
        keyBytes = '11111111111111'.bytes
        then:
        hGroup.isUseHH(keyBytes)
    }

    def 'test hdel'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hdel a field')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hdel a field')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hdel a field')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hdel a field')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hdel a field')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hdel a field')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.remove('field')
        100.times {
            rhk.add('field' + it)
        }
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hdel a field')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        rhh.remove('field')
        100.times {
            rhh.put('field' + it, ' '.bytes)
        }
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hdel a field')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = hGroup.execute('hdel >key field')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hdel a >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hello'() {
        given:
        def socket = SocketInspectorTest.mockTcpSocket()

        def hGroup = new HGroup('hello', null, socket)

        and:
        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        when:
        def reply = hGroup.execute('hello 2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hello 3')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hello 4')
        then:
        reply instanceof ErrorReply

        when:
        reply = hGroup.execute('hello 2 setname test')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hello 2 setname')
        then:
        reply == ErrorReply.SYNTAX

        when:
        def aclUsers = AclUsers.instance
        aclUsers.initForTest()
        reply = hGroup.execute('hello 2 auth user0 pass0')
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        aclUsers.upInsert('user0') { u ->
            u.on = false
            u.password = U.Password.NO_PASSWORD
        }
        reply = hGroup.execute('hello 2 auth user0 pass0')
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        aclUsers.upInsert('user0') { u ->
            u.on = true
            u.password = U.Password.plain('pass000')
        }
        reply = hGroup.execute('hello 2 auth user0 pass0')
        then:
        reply == ErrorReply.AUTH_FAILED

        when:
        aclUsers.upInsert('user0') { u ->
            u.on = true
            u.password = U.Password.plain('pass0')
        }
        reply = hGroup.execute('hello 2 auth user0 pass0')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hello 2 auth')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hello')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 14

        cleanup:
        localPersist.cleanUp()
    }

    def 'test hexists'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.execute('hexists a field')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hexists a field')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        reply = hGroup.execute('hexists a field')
        then:
        reply == IntegerReply.REPLY_1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hexists a field')
        then:
        reply == IntegerReply.REPLY_1

        when:
        reply = hGroup.execute('hexists a field1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = hGroup.execute('hexists >key field')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hexists a >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hget'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.execute('hstrlen a field')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hstrlen a field')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hget a field')
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hget a field')
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        reply = hGroup.execute('hstrlen a field')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == cvField.compressedData.length

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hstrlen a field')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hget a field')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == cvField.compressedData

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hget a field')
        then:
        reply instanceof BulkReply
        (reply as BulkReply).raw == ' '.bytes

        when:
        reply = hGroup.execute('hstrlen a field1')
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = hGroup.execute('hget a field1')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = hGroup.execute('hstrlen >key field')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hstrlen a >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hgetall'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hgetall a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hgetall a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvList = Mock.prepareCompressedValueList(2)
        def cvKeys = cvList[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()
        def cvField = cvList[1]
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        reply = hGroup.execute('hgetall a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'field'.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == cvField.compressedData

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hgetall a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'field'.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == ' '.bytes

        when:
        rhh.remove('field')
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hgetall a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        reply = hGroup.execute('hgetall a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'field'.bytes
        (reply as MultiBulkReply).replies[1] instanceof NilReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.remove('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hgetall a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = hGroup.execute('hgetall >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hincrby'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.execute('hincrby a field 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hincrby a field 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hincrbyfloat a field 1')
        then:
        reply instanceof DoubleReply
        (reply as DoubleReply).doubleValue() == 2.0d

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hincrbyfloat a field 1')
        then:
        reply instanceof DoubleReply
        (reply as DoubleReply).doubleValue() == 2.0d

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hincrby a field a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hincrbyfloat a field a')
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        reply = hGroup.execute('hincrby >key field 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hincrby a >key 1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', '0'.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hincrby a field 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        rhh.put('field', '1.1'.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hincrbyfloat a field 1')
        then:
        reply instanceof DoubleReply
        (reply as DoubleReply).doubleValue() == 2.1d

        when:
        rhh.remove('field')
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hincrbyfloat a field 1')
        then:
        reply instanceof DoubleReply
        (reply as DoubleReply).doubleValue() == 1.0d

        when:
        rhh.put('field', 'a'.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hincrbyfloat a field 1')
        then:
        reply == ErrorReply.NOT_FLOAT
    }

    def 'test hkeys'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hkeys a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hkeys a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hlen a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hlen a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hkeys a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'field'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hkeys a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'field'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hlen a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hlen a')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.remove('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hkeys a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        rhh.remove('field')
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hkeys a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = hGroup.execute('hlen a')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hkeys >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hmget'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hmget a field field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hmget a field field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvList = Mock.prepareCompressedValueList(2)
        def cvField = cvList[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        def cvField1 = cvList[1]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field1'), 0, cvField1)
        reply = hGroup.execute('hmget a field field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == cvField.compressedData
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == cvField1.compressedData

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        rhh.put('field1', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hmget a field field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == ' '.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == ' '.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hmget >key field field1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hmget a >key field1')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hmset'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hmset a field value')
        then:
        reply == OKReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hmset a field value')
        then:
        reply == OKReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        RedisHashKeys.HASH_MAX_SIZE.times {
            rhk.add('field' + it)
        }
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hmset a field value')
        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        RedisHashKeys.HASH_MAX_SIZE.times {
            rhh.put('field' + it, ' '.bytes)
        }
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hmset a field value')
        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.remove('field0')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hmset a field value field0 value0')
        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        rhh.remove('field0')
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hmset a field value field0 value0')
        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        10.times {
            rhk.remove('field' + it)
        }
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hmset a field value field0 value0')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = hGroup.execute('hset a field value field0 value0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        10.times {
            rhh.remove('field' + it)
        }
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hmset a field value field0 value0')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = hGroup.execute('hset a field value field0 value0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

        // get and compare
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hmget a field field0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'value'.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == 'value0'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hmget a field field0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == 'value'.bytes
        (reply as MultiBulkReply).replies[1] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[1]).raw == 'value0'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hmset >key field value')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hmset a >key value')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hmset a field >value')
        then:
        reply == ErrorReply.VALUE_TOO_LONG

        when:
        reply = hGroup.execute('hmset a _0 value')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hmset a field _0')
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test hrandfield'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hrandfield a 1 1')
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hrandfield a 1 1')
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hrandfield a 1 withvalues')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hrandfield a 1 withvalues')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hrandfield a 1 1')
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hrandfield a 1 1')
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hrandfield a 1 withvalues')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hrandfield a 1 withvalues')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        10.times {
            rhk.add('field' + it)
        }
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hrandfield a 1 1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        10.times {
            rhh.put('field' + it, ' '.bytes)
        }
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hrandfield a 1 1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        // > rhk size
        reply = hGroup.execute('hrandfield a 1 ' + (rhk.size() + 1))
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == rhk.size()

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hrandfield a 1 ' + (rhk.size() + 1))
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == rhh.size()

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hrandfield a 1 withvalues')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hrandfield a 1 withvalues')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        (reply as MultiBulkReply).replies[1] instanceof BulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvList = Mock.prepareCompressedValueList(rhk.size())
        rhk.size().times {
            inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field' + it), 0, cvList[it])
        }
        reply = hGroup.execute('hrandfield a 1 withvalues')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        (reply as MultiBulkReply).replies[1] instanceof BulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hrandfield a -1 withvalues')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        (reply as MultiBulkReply).replies[1] instanceof BulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hrandfield a -1 withvalues')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        (reply as MultiBulkReply).replies[1] instanceof BulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hrandfield a 5 5')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        reply = hGroup.execute('hrandfield a -5 -5')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 5

        when:
        reply = hGroup.execute('hrandfield >key -5 -5')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hrandfield a a -5')
        then:
        reply == ErrorReply.NOT_INTEGER
    }

    def 'test hscan'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hscan a 0 match field* count 5 novalues')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hscan a 0 match field* count 5 novalues')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hscan a 0 match field* count 5 novalues')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hscan a 0 match field* count 5 novalues')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        10.times {
            if (it == 5) {
                rhk.add('xxx')
            } else {
                rhk.add('field' + it)
            }
        }
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hscan a 0 match field* count 5 novalues')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 5

        when:
        reply = hGroup.execute('hscan a 0 match zzz count 5 novalues')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when:
        reply = hGroup.execute('hscan a 0 match zzz count 5 withvalues')
        then:
        reply == ErrorReply.NOT_SUPPORT

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        10.times {
            if (it == 5) {
                rhh.put('xxx', ' '.bytes)
            } else {
                rhh.put('field' + it, ' '.bytes)
            }
        }
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hscan a 0 match field* count 5 novalues')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 5

        when:
        reply = hGroup.execute('hscan a 0 match field* count 5 withvalues')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 10

        when:
        reply = hGroup.execute('hscan a 0 match zzz count 5 withvalues')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when:
        // invalid integer
        reply = hGroup.execute('hscan a 0 match zzz count a withvalues')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.execute('hscan a 0 match zzz count -1 withvalues')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = hGroup.execute('hscan a 0 match zzz xxx 10 withvalues')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hscan >key 0 match zzz count 10 withvalues')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hscan a 0 match field count')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hscan a 0 match field match')
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test hsetnx'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hsetnx a field value')
        then:
        reply == IntegerReply.REPLY_1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hsetnx a field value')
        then:
        reply == IntegerReply.REPLY_1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hsetnx a field value')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hsetnx a field value')
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hsetnx >key field value')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hsetnx a >key value')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hsetnx a field >value')
        then:
        reply == ErrorReply.VALUE_TOO_LONG

        when:
        reply = hGroup.execute('hsetnx a _0 value')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hsetnx a field _0')
        then:
        reply == ErrorReply.SYNTAX
    }

    def 'test hvals'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.execute('hvals a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hvals a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hvals a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hvals a')
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hvals a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hvals a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == ' '.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        reply = hGroup.execute('hvals a')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] instanceof BulkReply
        ((BulkReply) (reply as MultiBulkReply).replies[0]).raw == cvField.compressedData

        when:
        reply = hGroup.execute('hvals >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test httl'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def reply = hGroup.execute('httl a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        rhk.add('field0')
        rhk.add('field1')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('httl a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        def cvField0 = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field0'), 0, cvField0)
        def cvField1 = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field1'), 0, cvField1)
        reply = hGroup.execute('httl a fields 3 field0 field1 field2')
        reply = hGroup.execute('hpttl a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hexpiretime a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hpexpiretime a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hpersist a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        cvField0.expireAt = System.currentTimeMillis() + 1000 * 10
        cvField1.expireAt = System.currentTimeMillis() + 1000 * 10
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field0'), 0, cvField0)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field1'), 0, cvField1)
        reply = hGroup.execute('hpersist a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[1] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[2] == HGroup.FIELD_NOT_FOUND

        when:
        cvField0.expireAt = System.currentTimeMillis() + 1000 * 10
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field0'), 0, cvField0)
        reply = hGroup.execute('httl a fields 1 field0')
        reply = hGroup.execute('hpttl a fields 1 field0')
        reply = hGroup.execute('hexpiretime a fields 1 field0')
        reply = hGroup.execute('hpexpiretime a fields 1 field0')
        then:
        reply instanceof MultiBulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('httl a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('httl a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        rhh.put('field0', ' '.bytes)
        rhh.put('field1', ' '.bytes, System.currentTimeMillis() + 1000 * 10)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('httl a fields 3 field0 field1 field2')
        reply = hGroup.execute('hpttl a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hexpiretime a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hpexpiretime a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hpersist a fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('httl a fields 3 field0 field1')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('httl a fields 3x field0 field1 field2')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.execute('httl a fieldsx 3 field0 field1 field2')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('httl a fields 3')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = hGroup.execute('httl >key fields 1 field0')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('httl a fields 1 >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hexpire'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def reply = hGroup.execute('hexpire a 10 fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        rhk.add('field0')
        rhk.add('field1')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.execute('hexpire a 10 fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        def cvField0 = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field0'), 0, cvField0)
        def cvField1 = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field1'), 0, cvField1)
        reply = hGroup.execute('hexpire a 10 fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[1] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[2] == HGroup.FIELD_NOT_FOUND

        when:
        reply = hGroup.execute('hexpireat a ' + (System.currentTimeMillis() / 1000 + 10).intValue() + ' fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hpexpire a 10000 fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hpexpireat a ' + (System.currentTimeMillis() + 10000) + ' fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hexpire a 10 fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hexpire a 10 fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        rhh.put('field0', ' '.bytes)
        rhh.put('field1', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.execute('hexpire a 10 fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[1] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[2] == HGroup.FIELD_NOT_FOUND

        when:
        reply = hGroup.execute('hexpireat a ' + (System.currentTimeMillis() / 1000 + 10).intValue() + ' fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hpexpire a 10000 fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        when:
        reply = hGroup.execute('hpexpireat a ' + (System.currentTimeMillis() + 10000) + ' fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply

        // Test NX option
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        cvField0.expireAt = System.currentTimeMillis() + 1000 * 10
        cvField1.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field0'), 0, cvField0)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field1'), 0, cvField1)
        reply = hGroup.execute('hexpire a 20 nx fields 3 field0 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 3
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_0
        (reply as MultiBulkReply).replies[1] == IntegerReply.REPLY_1
        (reply as MultiBulkReply).replies[2] == HGroup.FIELD_NOT_FOUND

        // Test XX option
        when:
        cvField0.expireAt = CompressedValue.NO_EXPIRE
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field0'), 0, cvField0)
        reply = hGroup.execute('hexpire a 20 xx fields 1 field0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_0  // field0 has no expire

        // Test GT option
        when:
        cvField0.expireAt = System.currentTimeMillis() + 1000 * 5  // 5 seconds
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field0'), 0, cvField0)
        reply = hGroup.execute('hexpire a 3 gt fields 1 field0')  // 3 seconds < 5 seconds
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_0  // 3 < 5, so no update

        when:
        reply = hGroup.execute('hexpire a 10 gt fields 1 field0')  // 10 seconds > 5 seconds
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1  // 10 > 5, so update

        // Test LT option
        when:
        cvField0.expireAt = System.currentTimeMillis() + 1000 * 20  // 20 seconds
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field0'), 0, cvField0)
        reply = hGroup.execute('hexpire a 25 lt fields 1 field0')  // 25 seconds > 20 seconds
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_0  // 25 > 20, so no update

        when:
        reply = hGroup.execute('hexpire a 10 lt fields 1 field0')  // 10 seconds < 20 seconds
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1  // 10 < 20, so update

        // Test error cases
        when:
        reply = hGroup.execute('hexpire a 10x fields 3 field0 field1 field2')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.execute('hexpire a 10 fieldx 3 field0 field1 field2')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hexpire a 10 fields 3x field0 field1 field2')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.execute('hexpire a 10 fields 3 field0 field1')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hexpire >key 10 fields 1 field0')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hexpire a 10 fields 1 >key')
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
