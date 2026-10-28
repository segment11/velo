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
hgetdel
hgetex
hsetex
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

    /**
     * Puts a string value at the given key, so the hash commands see a key of another type.
     */
    private static void putStringKey(InMemoryGetSet inMemoryGetSet, short slot, String key) {
        def cv = Mock.prepareCompressedValueList(1)[0]
        cv.compressedData = 'hello'.bytes
        inMemoryGetSet.put(slot, key, 0, cv)
    }

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

    def 'test handle - format errors'() {
        given:
        def hGroup = new HGroup(null, null, null)
        hGroup.from(BaseCommand.mockAGroup())

        expect:
        hGroup.execute(cmd) == ErrorReply.FORMAT

        where:
        cmd << cmdLines
    }

    def 'test handle - hello and zzz'() {
        given:
        def hGroup = new HGroup(null, null, null)
        hGroup.from(BaseCommand.mockAGroup())

        expect:
        hGroup.execute('hello 4') instanceof ErrorReply
        hGroup.execute('zzz') == NilReply.INSTANCE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hdel wrongTypeKey field')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hexists wrongTypeKey field')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hget wrongTypeKey field')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hgetall wrongTypeKey')
        then:
        reply == ErrorReply.WRONG_TYPE
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

        // bug 78.1: split mode must create the hash field index, otherwise the
        // written field is invisible to every aggregate command
        when: 'split mode, the hash key does not exist yet'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashNew'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashNew', 'field'))
        reply = hGroup.execute('hincrby hashNew field 1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1
        // the field index is created
        def newKeysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashNew'), 0, 0)
        newKeysCv != null
        RedisHashKeys.decode(newKeysCv.cv().getCompressedData(), false).contains('field')
        // and all the aggregate commands see the field
        (hGroup.execute('hlen hashNew') as IntegerReply).integer == 1
        (hGroup.execute('hgetall hashNew') as MultiBulkReply).replies.length == 2
        // and the field can be removed again
        (hGroup.execute('hdel hashNew field') as IntegerReply).integer == 1

        // bug 78.2: the increment must support the whole int64 range
        when: 'split mode, an int64 delta'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashInt64'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashInt64', 'field'))
        reply = hGroup.execute('hincrby hashInt64 field 2147483648')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2147483648L

        when: 'split mode, the minimum int64 delta'
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashInt64Min'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashInt64Min', 'field'))
        reply = hGroup.execute('hincrby hashInt64Min field -2147483648')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -2147483648L

        when: 'HH mode, an int64 delta'
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashInt64HH')
        reply = hGroup.execute('hincrby hashInt64HH field 2147483648')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2147483648L

        when: 'HH mode, the minimum int64 delta'
        inMemoryGetSet.remove(slot, 'hashInt64HHMin')
        reply = hGroup.execute('hincrby hashInt64HHMin field -2147483648')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == -2147483648L

        when: 'a delta out of the int64 range'
        reply = hGroup.execute('hincrby hashInt64 field 99999999999999999999')
        then:
        reply == ErrorReply.NOT_INTEGER

        // bug 78.3: a non finite value must not escape as a raw exception
        when: 'HH mode, a delta that overflows to infinity'
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashNonFinite')
        reply = hGroup.execute('hincrbyfloat hashNonFinite field 1e400')
        then:
        reply == ErrorReply.NAN_OR_INFINITY

        when: 'HH mode, an infinite delta'
        reply = hGroup.execute('hincrbyfloat hashNonFinite field Infinity')
        then:
        reply == ErrorReply.NAN_OR_INFINITY

        when: 'HH mode, a NaN delta'
        reply = hGroup.execute('hincrbyfloat hashNonFinite field NaN')
        then:
        reply == ErrorReply.NAN_OR_INFINITY

        when: 'HH mode, the stored value is not finite'
        inMemoryGetSet.remove(slot, 'hashNonFinite')
        hGroup.execute('hset hashNonFinite field Infinity')
        reply = hGroup.execute('hincrbyfloat hashNonFinite field 1')
        then:
        reply == ErrorReply.NOT_FLOAT

        when: 'split mode, a delta that overflows to infinity'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashNonFinite'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashNonFinite', 'field'))
        reply = hGroup.execute('hincrbyfloat hashNonFinite field 1e400')
        then:
        reply == ErrorReply.NAN_OR_INFINITY

        when: 'split mode, the stored value is not finite'
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashNonFinite', 'field'))
        hGroup.execute('hset hashNonFinite field Infinity')
        reply = hGroup.execute('hincrbyfloat hashNonFinite field 1')
        then:
        reply == ErrorReply.NOT_FLOAT

        // bug 78.4: the HH layout must not silently wrap on overflow
        when: 'HH mode, the value overflows'
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashOverflow')
        hGroup.execute('hset hashOverflow field 9223372036854775807')
        reply = hGroup.execute('hincrby hashOverflow field 1')
        then:
        reply == ErrorReply.INCR_DECR_OVERFLOW

        when: 'HH mode, the value underflows'
        inMemoryGetSet.remove(slot, 'hashOverflow')
        hGroup.execute('hset hashOverflow field -9223372036854775808')
        reply = hGroup.execute('hincrby hashOverflow field -1')
        then:
        reply == ErrorReply.INCR_DECR_OVERFLOW

        when: 'split mode, the value overflows'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashOverflow'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashOverflow', 'field'))
        hGroup.execute('hset hashOverflow field 9223372036854775807')
        reply = hGroup.execute('hincrby hashOverflow field 1')
        then:
        reply == ErrorReply.INCR_DECR_OVERFLOW

        when: 'HH mode, a negative delta that does not overflow'
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashOverflow')
        hGroup.execute('hset hashOverflow field 10')
        reply = hGroup.execute('hincrby hashOverflow field -1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 9L

        when: 'HH mode, a zero delta'
        reply = hGroup.execute('hincrby hashOverflow field 0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 9L
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hincrby wrongTypeKey field 1')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hkeys wrongTypeKey')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hmget wrongTypeKey field')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        (reply as IntegerReply).integer == 0

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
        (reply as IntegerReply).integer == 0

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

        // bug 78.5: overwriting the value clears the field ttl, like redis does
        when: 'HH mode, the field has a ttl and the value is overwritten'
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashTtl')
        hGroup.execute('hset hashTtl field v1')
        hGroup.execute('hexpire hashTtl 60 fields 1 field')
        def ttlBefore = (hGroup.execute('httl hashTtl fields 1 field') as MultiBulkReply).replies[0]
        hGroup.execute('hset hashTtl field v2')
        def ttlAfter = (hGroup.execute('httl hashTtl fields 1 field') as MultiBulkReply).replies[0]
        then:
        (ttlBefore as IntegerReply).integer > 0
        (ttlAfter as IntegerReply).integer == -1

        when: 'HH mode, hmset overwrites the value of a field with a ttl'
        inMemoryGetSet.remove(slot, 'hashTtl')
        hGroup.execute('hset hashTtl field v1')
        hGroup.execute('hexpire hashTtl 60 fields 1 field')
        hGroup.execute('hmset hashTtl field v2')
        def ttlAfterHmset = (hGroup.execute('httl hashTtl fields 1 field') as MultiBulkReply).replies[0]
        then:
        (ttlAfterHmset as IntegerReply).integer == -1

        when: 'split mode, the field has a ttl and the value is overwritten'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashTtl'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashTtl', 'field'))
        hGroup.execute('hset hashTtl field v1')
        hGroup.execute('hexpire hashTtl 60 fields 1 field')
        hGroup.execute('hset hashTtl field v2')
        def splitTtlAfter = (hGroup.execute('httl hashTtl fields 1 field') as MultiBulkReply).replies[0]
        then:
        (splitTtlAfter as IntegerReply).integer == -1
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hmset wrongTypeKey field value')
        then:
        reply == ErrorReply.WRONG_TYPE
    }

    def 'test hset returns newly added field count'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def reply

        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        LocalPersist.instance.hashSaveMemberTogether = false

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)
        reply = hGroup.execute('hset hashA field1 val1')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = hGroup.execute('hset hashA field1 val2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0

        when:
        reply = hGroup.execute('hset hashA field1 val3 field2 val4')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = hGroup.execute('hset hashA field1 val5 field2 val6 field3 val7')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when:
        reply = hGroup.execute('hset hashA field1 val8 field2 val9 field3 val10 field4 val11')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hrandfield wrongTypeKey')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        reply = hGroup.execute('hscan a 0 match zzz count 5')
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
        reply = hGroup.execute('hscan a 0 match field* count 5')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[1] instanceof MultiBulkReply
        ((MultiBulkReply) (reply as MultiBulkReply).replies[1]).replies.length == 10

        when:
        reply = hGroup.execute('hscan a 0 match zzz count 5')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        when:
        // invalid integer
        reply = hGroup.execute('hscan a 0 match zzz count a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.execute('hscan a 0 match zzz count -1')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        // Redis rejects COUNT 0, accepting it silently acted as COUNT 1 and corrupted the client cursor
        reply = hGroup.execute('hscan a 0 count 0')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = hGroup.execute('hscan a 0 count 0 novalues')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        // WITHVALUES is not a Redis HSCAN option, the default form already returns field/value pairs
        reply = hGroup.execute('hscan a 0 withvalues')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hscan a 0 match zzz xxx 10')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hscan >key 0 match zzz count 10')
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

        when:
        reply = hGroup.execute('hscan a badcursor match field count 10')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        // A key holding another type must answer WRONGTYPE, not an empty scan.
        // Split mode never reads the main key, so it needs an explicit type check.
        def cvWrongType = Mock.prepareCompressedValueList(1)[0]
        cvWrongType.dictSeqOrSpType = CompressedValue.SP_TYPE_SET
        cvWrongType.compressedData = new RedisHashKeys().encode()
        inMemoryGetSet.put(slot, 'a', 0, cvWrongType)
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hscan a 0 count 10 novalues')
        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hscan a 0 count 10 novalues')
        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        // A string key is a wrong type for HSCAN in both storage modes
        def cvString = Mock.prepareCompressedValueList(1)[0]
        cvString.dictSeqOrSpType = CompressedValue.SP_TYPE_SHORT_STRING
        cvString.compressedData = 'v'.bytes
        inMemoryGetSet.put(slot, 'a', 0, cvString)
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.execute('hscan a 0 count 10 novalues')
        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.execute('hscan a 0 count 10 novalues')
        then:
        reply == ErrorReply.WRONG_TYPE

        when:
        // A missing key still answers an empty scan, like Redis
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.execute('hscan a 0 count 10 novalues')
        then:
        reply == MultiBulkReply.SCAN_EMPTY

        cleanup:
        LocalPersist.instance.hashSaveMemberTogether = false
    }

    def 'test hgetdel'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())
        LocalPersist.instance.hashSaveMemberTogether = false

        when:
        def reply = hGroup.execute('hgetdel hashA FIELDS -1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.execute('hgetdel hashA FIELDS abc field1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = hGroup.execute('hgetdel hashA FIELDS 1 field extra')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = hGroup.execute('hgetdel >key FIELDS 1 field1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hgetdel hashA')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = hGroup.execute('hgetdel hashA notfields 1 field1')
        then:
        reply == ErrorReply.SYNTAX

        when:
        def tooLongField = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        Arrays.fill(tooLongField, (byte) 'a')
        reply = hGroup.execute('hgetdel hashA FIELDS 1 ' + new String(tooLongField))
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        reply = hGroup.execute('hgetdel hashA FIELDS 2 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')
        rhk.add('field3')
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        def cvField1 = Mock.prepareCompressedValueList(1)[0]
        cvField1.compressedData = 'value1'.bytes
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field1'), 0, cvField1)

        def cvField2 = Mock.prepareCompressedValueList(1)[0]
        cvField2.compressedData = 'value2'.bytes
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field2'), 0, cvField2)

        reply = hGroup.execute('hgetdel hashA FIELDS 2 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        new String(((reply as MultiBulkReply).replies[0] as BulkReply).raw) == 'value1'
        new String(((reply as MultiBulkReply).replies[1] as BulkReply).raw) == 'value2'

        and:
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        !savedRHK.contains('field1')
        !savedRHK.contains('field2')
        savedRHK.contains('field3')

        when:
        cvKeys = Mock.prepareCompressedValueList(1)[0]
        rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')
        rhk.add('field3')
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        cvField1 = Mock.prepareCompressedValueList(1)[0]
        cvField1.compressedData = 'value1'.bytes
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field1'), 0, cvField1)

        cvField2 = Mock.prepareCompressedValueList(1)[0]
        cvField2.compressedData = 'value2'.bytes
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field2'), 0, cvField2)

        reply = hGroup.execute('hgetdel hashA FIELDS 2 field1 nonexistent')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        new String(((reply as MultiBulkReply).replies[0] as BulkReply).raw) == 'value1'
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashA')
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field1', 'value1'.bytes)
        rhh.put('field2', 'value2'.bytes)
        rhh.put('field3', 'value3'.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'hashA', 0, cvRhh)

        reply = hGroup.execute('hgetdel hashA FIELDS 2 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        new String(((reply as MultiBulkReply).replies[0] as BulkReply).raw) == 'value1'
        new String(((reply as MultiBulkReply).replies[1] as BulkReply).raw) == 'value2'

        and:
        def rhhCv = inMemoryGetSet.getBuf(slot, 'hashA', 0, 0)
        def savedRhh = RedisHH.decode(rhhCv.cv().getCompressedData())
        savedRhh.get('field1') == null
        savedRhh.get('field2') == null
        savedRhh.get('field3') != null

        when: 'test HH mode rhh is null (key does not exist)'
        inMemoryGetSet.remove(slot, 'hashA')
        reply = hGroup.execute('hgetdel hashA FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE

        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hgetdel wrongTypeKey fields 1 field')
        then:
        reply == ErrorReply.WRONG_TYPE
        cleanup:
        LocalPersist.instance.hashSaveMemberTogether = false
    }

    def 'test hgetex'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())
        LocalPersist.instance.hashSaveMemberTogether = false

        when: 'test numFields negative'
        def reply = hGroup.execute('hgetex hashA FIELDS -1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test extra args after fields'
        reply = hGroup.execute('hgetex hashA FIELDS 1 field extra')
        then:
        reply == ErrorReply.SYNTAX

        when: 'test format error - not enough args'
        reply = hGroup.execute('hgetex hashA')
        then:
        reply == ErrorReply.FORMAT

        when: 'test numFields invalid integer'
        reply = hGroup.execute('hgetex hashA FIELDS abc')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test key too long'
        reply = hGroup.execute('hgetex >key FIELDS 1 field1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'test format error - fields count mismatch'
        reply = hGroup.execute('hgetex hashA FIELDS 2 field1')
        then:
        reply == ErrorReply.SYNTAX

        when: 'test rhk is null (key does not exist)'
        reply = hGroup.execute('hgetex nonexistent FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE

        when: 'setup hash data for non-HH mode'
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        def rhk = new RedisHashKeys()
        rhk.add('field1')
        rhk.add('field2')
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        def cvField1 = Mock.prepareCompressedValueList(1)[0]
        cvField1.compressedData = 'value1'.bytes
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field1'), 0, cvField1)

        def cvField2 = Mock.prepareCompressedValueList(1)[0]
        cvField2.compressedData = 'value2'.bytes
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field2'), 0, cvField2)

        reply = hGroup.execute('hgetex hashA FIELDS 2 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        new String(((reply as MultiBulkReply).replies[0] as BulkReply).raw) == 'value1'
        new String(((reply as MultiBulkReply).replies[1] as BulkReply).raw) == 'value2'

        when: 'test EX option'
        reply = hGroup.execute('hgetex hashA EX 3600 FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        new String(((reply as MultiBulkReply).replies[0] as BulkReply).raw) == 'value1'
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        savedRHK.getCachedExpireAt('field1') > System.currentTimeMillis()

        when: 'test nonexistent field'
        reply = hGroup.execute('hgetex hashA FIELDS 2 field1 nonexistent')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        new String(((reply as MultiBulkReply).replies[0] as BulkReply).raw) == 'value1'
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE

        when: 'test EX with nonexistent field'
        reply = hGroup.execute('hgetex hashA EX 3600 FIELDS 2 field1 nonexistent')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        new String(((reply as MultiBulkReply).replies[0] as BulkReply).raw) == 'value1'
        (reply as MultiBulkReply).replies[1] == NilReply.INSTANCE
        def keysCv2 = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK2 = RedisHashKeys.decode(keysCv2.cv().getCompressedData(), false)
        savedRHK2.getCachedExpireAt('field1') > System.currentTimeMillis()

        when: 'test invalid EX value (non-integer)'
        reply = hGroup.execute('hgetex hashA EX abc FIELDS 1 field1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test invalid PX value (non-integer)'
        reply = hGroup.execute('hgetex hashA PX abc FIELDS 1 field1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test invalid EXAT value (non-integer)'
        reply = hGroup.execute('hgetex hashA EXAT abc FIELDS 1 field1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test invalid PXAT value (non-integer)'
        reply = hGroup.execute('hgetex hashA PXAT abc FIELDS 1 field1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test invalid expire option'
        reply = hGroup.execute('hgetex hashA XX 3600 FIELDS 1 field1')
        then:
        reply == ErrorReply.SYNTAX

        when: 'test missing FIELDS keyword after expire option'
        reply = hGroup.execute('hgetex hashA EX 3600 1 field1')
        then:
        reply == ErrorReply.SYNTAX

        when: 'test switch to HH mode'
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashA')
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field1', 'value1'.bytes)
        rhh.put('field2', 'value2'.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'hashA', 0, cvRhh)

        reply = hGroup.execute('hgetex hashA FIELDS 2 field1 field2')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 2
        new String(((reply as MultiBulkReply).replies[0] as BulkReply).raw) == 'value1'
        new String(((reply as MultiBulkReply).replies[1] as BulkReply).raw) == 'value2'

        when: 'test HH mode EX option'
        reply = hGroup.execute('hgetex hashA EX 3600 FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        def rhhCv = inMemoryGetSet.getBuf(slot, 'hashA', 0, 0)
        def savedRhh = RedisHH.decode(rhhCv.cv().getCompressedData())
        savedRhh.mapExpireAt.get('field1') > System.currentTimeMillis()

        when: 'test HH mode PX option'
        reply = hGroup.execute('hgetex hashA PX 3600000 FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        def rhhCvPx = inMemoryGetSet.getBuf(slot, 'hashA', 0, 0)
        def savedRhhPx = RedisHH.decode(rhhCvPx.cv().getCompressedData())
        savedRhhPx.mapExpireAt.get('field1') > System.currentTimeMillis() + 3600000 - 10000

        when: 'test HH mode PXAT option'
        def pxatTime = System.currentTimeMillis() + 3600000
        reply = hGroup.execute('hgetex hashA PXAT ' + pxatTime + ' FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        def rhhCvPxat = inMemoryGetSet.getBuf(slot, 'hashA', 0, 0)
        def savedRhhPxat = RedisHH.decode(rhhCvPxat.cv().getCompressedData())
        savedRhhPxat.mapExpireAt.get('field1') == pxatTime

        when: 'test HH mode EXAT option'
        def exatTime = ((System.currentTimeMillis() / 1000) as long) + 3600
        reply = hGroup.execute('hgetex hashA EXAT ' + exatTime + ' FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        def rhhCvExat = inMemoryGetSet.getBuf(slot, 'hashA', 0, 0)
        def savedRhhExat = RedisHH.decode(rhhCvExat.cv().getCompressedData())
        savedRhhExat.mapExpireAt.get('field1') == exatTime * 1000

        when: 'test HH mode PERSIST option'
        reply = hGroup.execute('hgetex hashA PERSIST FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        def rhhCvPersist = inMemoryGetSet.getBuf(slot, 'hashA', 0, 0)
        def savedRhhPersist = RedisHH.decode(rhhCvPersist.cv().getCompressedData())
        !savedRhhPersist.mapExpireAt.containsKey('field1')

        when: 'test HH mode with null rhh (key does not exist)'
        inMemoryGetSet.remove(slot, 'hashA')
        reply = hGroup.execute('hgetex hashA EX 3600 FIELDS 1 field1')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies.length == 1
        (reply as MultiBulkReply).replies[0] == NilReply.INSTANCE

        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hgetex wrongTypeKey fields 1 field')
        then:
        reply == ErrorReply.WRONG_TYPE
        cleanup:
        LocalPersist.instance.hashSaveMemberTogether = false
    }

    def 'test hsetex'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())
        LocalPersist.instance.hashSaveMemberTogether = false

        when:
        def reply = hGroup.execute('hsetex hashA FIELDS 2 field1 value1 field2 value2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        // Verify fields were set
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        savedRHK.contains('field1')
        savedRHK.contains('field2')

        def field1Cv = inMemoryGetSet.getBuf(slot, RedisHashKeys.fieldKey('hashA', 'field1'), 0, 0)
        new String(field1Cv.cv().getCompressedData()) == 'value1'

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashA')
        reply = hGroup.execute('hsetex hashA FIELDS 2 field1 value1 field2 value2')
        then:
        (reply as IntegerReply).integer == 1

        and:
        def rhhCv = inMemoryGetSet.getBuf(slot, 'hashA', 0, 0)
        def savedRhh = RedisHH.decode(rhhCv.cv().getCompressedData())
        savedRhh.get('field1') != null
        savedRhh.get('field2') != null

        when: 'test format error - not enough args'
        reply = hGroup.execute('hsetex hashA')
        then:
        reply == ErrorReply.FORMAT

        when: 'test key too long'
        reply = hGroup.execute('hsetex >key FIELDS 2 field1 value1 field2 value2')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'test fnx option - field does not exist so should set'
        reply = hGroup.execute('hsetex hashA fnx FIELDS 1 newfield newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test fxx option - field exists so should set'
        reply = hGroup.execute('hsetex hashA fxx FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        // bug 78.9: an expired field does not exist, redis answers 1 for fnx and
        // 0 for fxx
        when: 'split mode, the field exists and is live, fnx does not set it'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashFnx'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashFnx', 'field'))
        hGroup.execute('hset hashFnx field value')
        reply = hGroup.execute('hsetex hashFnx fnx FIELDS 1 field value2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        (hGroup.execute('hget hashFnx field') as BulkReply).raw == 'value'.bytes

        when: 'split mode, the field exists and is live, fxx sets it'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashFxx'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashFxx', 'field'))
        hGroup.execute('hset hashFxx field value')
        reply = hGroup.execute('hsetex hashFxx fxx FIELDS 1 field value2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1
        (hGroup.execute('hget hashFxx field') as BulkReply).raw == 'value2'.bytes

        when: 'split mode, the field is expired and fnx is used'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashExpired'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashExpired', 'field'))
        hGroup.execute('hset hashExpired field value')
        hGroup.execute("hpexpireat hashExpired ${System.currentTimeMillis() - 60000} fields 1 field")
        reply = hGroup.execute('hsetex hashExpired fnx FIELDS 1 field value2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1
        (hGroup.execute('hget hashExpired field') as BulkReply).raw == 'value2'.bytes
        (hGroup.execute('hlen hashExpired') as IntegerReply).integer == 1

        when: 'split mode, the field is expired and fxx is used'
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashExpired'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashExpired', 'field'))
        hGroup.execute('hset hashExpired field value')
        hGroup.execute("hpexpireat hashExpired ${System.currentTimeMillis() - 60000} fields 1 field")
        reply = hGroup.execute('hsetex hashExpired fxx FIELDS 1 field value2')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        hGroup.execute('hget hashExpired field') == NilReply.INSTANCE

        // bug 78.7: fnx and fxx are hash level conditions, all the fields are set
        // or none of them is set
        when: 'split mode, fnx with one existing field sets nothing'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashFnxAll'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashFnxAll', 'a'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashFnxAll', 'b'))
        hGroup.execute('hset hashFnxAll a 1')
        reply = hGroup.execute('hsetex hashFnxAll fnx FIELDS 2 a 9 b 8')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        (hGroup.execute('hget hashFnxAll a') as BulkReply).raw == '1'.bytes
        hGroup.execute('hget hashFnxAll b') == NilReply.INSTANCE

        when: 'split mode, fxx with one missing field sets nothing'
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashFxxAll'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashFxxAll', 'a'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashFxxAll', 'b'))
        hGroup.execute('hset hashFxxAll a 1')
        reply = hGroup.execute('hsetex hashFxxAll fxx FIELDS 2 a 9 b 8')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        (hGroup.execute('hget hashFxxAll a') as BulkReply).raw == '1'.bytes
        hGroup.execute('hget hashFxxAll b') == NilReply.INSTANCE

        when: 'HH mode, fnx with one existing field sets nothing'
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashFnxAllHH')
        hGroup.execute('hset hashFnxAllHH a 1')
        reply = hGroup.execute('hsetex hashFnxAllHH fnx FIELDS 2 a 9 b 8')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        (hGroup.execute('hget hashFnxAllHH a') as BulkReply).raw == '1'.bytes
        hGroup.execute('hget hashFnxAllHH b') == NilReply.INSTANCE

        when: 'HH mode, fxx with one missing field sets nothing'
        inMemoryGetSet.remove(slot, 'hashFxxAllHH')
        hGroup.execute('hset hashFxxAllHH a 1')
        reply = hGroup.execute('hsetex hashFxxAllHH fxx FIELDS 2 a 9 b 8')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        (hGroup.execute('hget hashFxxAllHH a') as BulkReply).raw == '1'.bytes
        hGroup.execute('hget hashFxxAllHH b') == NilReply.INSTANCE

        when: 'split mode, fxx on a key that does not exist sets nothing'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashFxxNone'))
        reply = hGroup.execute('hsetex hashFxxNone fxx FIELDS 1 field value')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        hGroup.execute('hget hashFxxNone field') == NilReply.INSTANCE

        when: 'HH mode, fxx on a key that does not exist sets nothing'
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'hashFxxNoneHH')
        reply = hGroup.execute('hsetex hashFxxNoneHH fxx FIELDS 1 field value')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0
        hGroup.execute('hget hashFxxNoneHH field') == NilReply.INSTANCE

        when: 'test EX option invalid integer'
        reply = hGroup.execute('hsetex hashA EX abc FIELDS 1 field1 value1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test PX option invalid integer'
        reply = hGroup.execute('hsetex hashA PX abc FIELDS 1 field1 value1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test EXAT option invalid integer'
        reply = hGroup.execute('hsetex hashA EXAT abc FIELDS 1 field1 value1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test PXAT option invalid integer'
        reply = hGroup.execute('hsetex hashA PXAT abc FIELDS 1 field1 value1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test syntax error - missing fields keyword'
        reply = hGroup.execute('hsetex hashA EX 3600 1 field1 value1')
        then:
        reply == ErrorReply.SYNTAX

        when: 'test syntax error - numFields not integer'
        reply = hGroup.execute('hsetex hashA FIELDS abc field1 value1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test syntax error - numFields negative'
        reply = hGroup.execute('hsetex hashA FIELDS -1 field1 value1')
        then:
        reply == ErrorReply.NOT_INTEGER

        when: 'test syntax error - field count mismatch'
        reply = hGroup.execute('hsetex hashA FIELDS 2 field1')
        then:
        reply == ErrorReply.SYNTAX

        when: 'test field too long'
        def tooLongField = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        Arrays.fill(tooLongField, (byte) 'a')
        reply = hGroup.execute('hsetex hashA FIELDS 1 ' + new String(tooLongField) + ' value1')
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when: 'test keepttl option'
        reply = hGroup.execute('hsetex hashA keepttl FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test normal mode with EX option'
        reply = hGroup.execute('hsetex hashA EX 3600 FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test normal mode with PX option'
        reply = hGroup.execute('hsetex hashA PX 3600000 FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test normal mode with PXAT option'
        def pxatTime = System.currentTimeMillis() + 3600000
        reply = hGroup.execute('hsetex hashA PXAT ' + pxatTime + ' FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test normal mode with EXAT option'
        def exatTime = ((System.currentTimeMillis() / 1000) as long) + 3600
        reply = hGroup.execute('hsetex hashA EXAT ' + exatTime + ' FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test HH mode with EX option'
        reply = hGroup.execute('hsetex hashA EX 3600 FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test HH mode with PX option'
        reply = hGroup.execute('hsetex hashA PX 3600000 FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test HH mode with PXAT option'
        pxatTime = System.currentTimeMillis() + 3600000
        reply = hGroup.execute('hsetex hashA PXAT ' + pxatTime + ' FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test HH mode with EXAT option'
        exatTime = ((System.currentTimeMillis() / 1000) as long) + 3600
        reply = hGroup.execute('hsetex hashA EXAT ' + exatTime + ' FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        when: 'test HH mode with keepttl option'
        reply = hGroup.execute('hsetex hashA keepttl FIELDS 1 field1 newvalue')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hsetex wrongTypeKey fields 1 field value')
        then:
        reply == ErrorReply.WRONG_TYPE
        cleanup:
        LocalPersist.instance.hashSaveMemberTogether = false
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

        // bug 78.9: an expired field does not exist, redis creates it again
        when: 'split mode, the field is expired'
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('hashExpired'))
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('hashExpired', 'field'))
        hGroup.execute('hset hashExpired field value')
        hGroup.execute("hpexpireat hashExpired ${System.currentTimeMillis() - 60000} fields 1 field")
        reply = hGroup.execute('hsetnx hashExpired field value2')
        then:
        reply == IntegerReply.REPLY_1
        (hGroup.execute('hget hashExpired field') as BulkReply).raw == 'value2'.bytes
        // the field is live again, the stale cached ttl must not hide it
        (hGroup.execute('hlen hashExpired') as IntegerReply).integer == 1

        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hsetnx wrongTypeKey field value')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hvals wrongTypeKey')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('httl wrongTypeKey fields 1 field')
        then:
        reply == ErrorReply.WRONG_TYPE
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
        // bug 78.8: the split layout stores the hash out of line, the type of the
        // main key must be checked, redis answers WRONGTYPE for another type
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        putStringKey(inMemoryGetSet, slot, 'wrongTypeKey')
        reply = hGroup.execute('hexpire wrongTypeKey 10 fields 1 field')
        then:
        reply == ErrorReply.WRONG_TYPE
    }

    def 'test hexpire writes cache and persists to cv storage'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        // Set up hash with 2 fields
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        def rhk = new RedisHashKeys()
        rhk.add('field0')
        rhk.add('field1')
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def encoded = rhk.encodeButDoNotCompress()
        cvKeys.compressedData = encoded
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        def cvField0 = Mock.prepareCompressedValueList(1)[0]
        cvField0.compressedData = 'val0'.bytes
        cvField0.setExpireAt(CompressedValue.NO_EXPIRE)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, cvField0)

        def cvField1 = Mock.prepareCompressedValueList(1)[0]
        cvField1.compressedData = 'val1'.bytes
        cvField1.setExpireAt(CompressedValue.NO_EXPIRE)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field1'), 0, cvField1)

        when:
        def reply = hGroup.execute('hexpire hashA 100 fields 1 field0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1

        // Verify CV storage was updated
        def savedCvField0 = inMemoryGetSet.getBuf(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, 0)
        savedCvField0.cv().getExpireAt() > System.currentTimeMillis()

        // Verify cache was updated
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        savedRHK.getCachedExpireAt('field0') > System.currentTimeMillis()
        savedRHK.getCachedExpireAt('field1') == CompressedValue.NO_EXPIRE
    }

    def 'test hpersist clears cache and cv storage'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        // Set up hash with 2 fields, field0 has TTL
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        def rhk = new RedisHashKeys()
        rhk.add('field0')
        rhk.add('field1')
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def encoded = rhk.encodeButDoNotCompress()
        cvKeys.compressedData = encoded
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        def futureExpireAt = System.currentTimeMillis() + 3600000
        def cvField0 = Mock.prepareCompressedValueList(1)[0]
        cvField0.compressedData = 'val0'.bytes
        cvField0.setExpireAt(futureExpireAt)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, cvField0)

        def cvField1 = Mock.prepareCompressedValueList(1)[0]
        cvField1.compressedData = 'val1'.bytes
        cvField1.setExpireAt(CompressedValue.NO_EXPIRE)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field1'), 0, cvField1)

        when:
        def reply = hGroup.execute('hpersist hashA fields 1 field0')
        then:
        reply instanceof MultiBulkReply
        (reply as MultiBulkReply).replies[0] == IntegerReply.REPLY_1

        // Verify CV storage was updated
        def savedCvField0 = inMemoryGetSet.getBuf(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, 0)
        savedCvField0.cv().getExpireAt() == CompressedValue.NO_EXPIRE

        // Verify cache was updated
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        savedRHK.getCachedExpireAt('field0') == CompressedValue.NO_EXPIRE
        savedRHK.getCachedExpireAt('field1') == CompressedValue.NO_EXPIRE
    }

    def 'test hdel removes field and its cached ttl'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        // Set up hash with 2 fields
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        def rhk = new RedisHashKeys()
        rhk.add('field0')
        rhk.add('field1')
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def encoded = rhk.encodeButDoNotCompress()
        cvKeys.compressedData = encoded
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        def cvField0 = Mock.prepareCompressedValueList(1)[0]
        cvField0.compressedData = 'val0'.bytes
        cvField0.setExpireAt(System.currentTimeMillis() + 3600000)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, cvField0)

        def cvField1 = Mock.prepareCompressedValueList(1)[0]
        cvField1.compressedData = 'val1'.bytes
        cvField1.setExpireAt(CompressedValue.NO_EXPIRE)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field1'), 0, cvField1)

        when:
        def reply = hGroup.execute('hdel hashA field0')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 1

        // Verify field was deleted from cache
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        savedRHK.getCachedExpireAt('field0') == CompressedValue.NO_EXPIRE // removed from cache
        savedRHK.getCachedExpireAt('field1') == CompressedValue.NO_EXPIRE
    }

    def 'test hset clears cached ttl for the field'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        // Set up hash with 1 field that has TTL
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        def rhk = new RedisHashKeys()
        rhk.add('field0')
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def encoded = rhk.encodeButDoNotCompress()
        cvKeys.compressedData = encoded
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        def futureExpireAt = System.currentTimeMillis() + 3600000
        def cvField0 = Mock.prepareCompressedValueList(1)[0]
        cvField0.compressedData = 'val0'.bytes
        cvField0.setExpireAt(futureExpireAt)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, cvField0)

        when:
        def reply = hGroup.execute('hset hashA field0 newval')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 0 // field existed, not newly added

        // Verify cached TTL was cleared
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        savedRHK.getCachedExpireAt('field0') == CompressedValue.NO_EXPIRE
    }

    def 'test hincrby keeps the cached ttl in sync with the field cv ttl'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())
        LocalPersist.instance.hashSaveMemberTogether = false

        // Set up hash with 1 field that has cached TTL
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        def rhk = new RedisHashKeys()
        rhk.add('field0')
        rhk.putCachedExpireAt('field0', System.currentTimeMillis() + 3600000)
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def encoded = rhk.encodeButDoNotCompress()
        cvKeys.compressedData = encoded
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        // Set up numeric field CV
        def cvField0 = Mock.prepareCompressedValueList(1)[0]
        cvField0.compressedData = '5'.bytes
        cvField0.setExpireAt(System.currentTimeMillis() + 3600000)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, cvField0)

        when:
        def reply = hGroup.execute('hincrby hashA field0 3')
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 8

        // Redis keeps the field ttl on hincrby, so the cached ttl must be kept too
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        savedRHK.getCachedExpireAt('field0') > System.currentTimeMillis()

        // and the field cv ttl is not changed either
        def fieldCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, 0)
        fieldCv.cv().getExpireAt() > System.currentTimeMillis()
    }

    def 'test hincrby on non-numeric value does not clear cached ttl'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def hGroup = new HGroup(null, null, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        // Set up hash with 1 field that has cached TTL
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        def rhk = new RedisHashKeys()
        rhk.add('field0')
        rhk.putCachedExpireAt('field0', System.currentTimeMillis() + 3600000)
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def encoded = rhk.encodeButDoNotCompress()
        cvKeys.compressedData = encoded
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('hashA'), 0, cvKeys)

        // Set up non-numeric field CV
        def cvField0 = Mock.prepareCompressedValueList(1)[0]
        cvField0.compressedData = 'notanumber'.bytes
        cvField0.setExpireAt(System.currentTimeMillis() + 3600000)
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('hashA', 'field0'), 0, cvField0)

        when:
        def reply = hGroup.execute('hincrby hashA field0 3')
        then:
        reply == ErrorReply.NOT_INTEGER

        // Verify cached TTL was NOT cleared (operation failed)
        def keysCv = inMemoryGetSet.getBuf(slot, RedisHashKeys.keysKey('hashA'), 0, 0)
        def savedRHK = RedisHashKeys.decode(keysCv.cv().getCompressedData(), false)
        savedRHK.getCachedExpireAt('field0') > System.currentTimeMillis()
    }

}
