package io.velo.command

import io.velo.BaseCommand
import io.velo.CompressedValue
import io.velo.mock.InMemoryGetSet
import io.velo.persist.LocalPersist
import io.velo.persist.Mock
import io.velo.reply.*
import io.velo.type.RedisHH
import io.velo.type.RedisHashKeys
import spock.lang.Specification

class HGroupTest extends Specification {
    def _HGroup = new HGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sHdelList = _HGroup.parseSlots('hdel', data2, slotNumber)
        def sHexistsList = _HGroup.parseSlots('hexists', data2, slotNumber)
        def sHgetList = _HGroup.parseSlots('hget', data2, slotNumber)
        def sHgetallList = _HGroup.parseSlots('hgetall', data2, slotNumber)
        def sHincrbyList = _HGroup.parseSlots('hincrby', data2, slotNumber)
        def sHincrbyfloatList = _HGroup.parseSlots('hincrbyfloat', data2, slotNumber)
        def sHkeysList = _HGroup.parseSlots('hkeys', data2, slotNumber)
        def sHlenList = _HGroup.parseSlots('hlen', data2, slotNumber)
        def sHmgetList = _HGroup.parseSlots('hmget', data2, slotNumber)
        def sHmsetList = _HGroup.parseSlots('hmset', data2, slotNumber)
        def sHrandfieldList = _HGroup.parseSlots('hrandfield', data2, slotNumber)
        def sHsetList = _HGroup.parseSlots('hset', data2, slotNumber)
        def sHsetnxList = _HGroup.parseSlots('hsetnx', data2, slotNumber)
        def sHstrlenList = _HGroup.parseSlots('hstrlen', data2, slotNumber)
        def sHvalsList = _HGroup.parseSlots('hvals', data2, slotNumber)
        def sList = _HGroup.parseSlots('hxxx', data2, slotNumber)
        then:
        sHdelList.size() == 1
        sHexistsList.size() == 1
        sHgetList.size() == 1
        sHgetallList.size() == 1
        sHincrbyList.size() == 1
        sHincrbyfloatList.size() == 1
        sHkeysList.size() == 1
        sHlenList.size() == 1
        sHmgetList.size() == 1
        sHmsetList.size() == 1
        sHrandfieldList.size() == 1
        sHsetList.size() == 1
        sHsetnxList.size() == 1
        sHstrlenList.size() == 1
        sHvalsList.size() == 1
        sList.size() == 0

        when:
        def data1 = new byte[1][]
        sHgetList = _HGroup.parseSlots('hget', data1, slotNumber)
        then:
        sHgetList.size() == 0
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
        hGroup.cmd = 'hexists'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hget'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hgetall'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hincrby'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hincrbyfloat'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hkeys'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hlen'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hmget'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hmset'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data5 = new byte[5][]
        hGroup.data = data5
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.data = data1
        hGroup.cmd = 'hrandfield'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hset'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hsetnx'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hstrlen'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        hGroup.cmd = 'hvals'
        reply = hGroup.handle()
        then:
        reply == ErrorReply.FORMAT

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
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'field'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hdel', data3, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hdel', data3, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hdel()
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hdel()
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
        reply = hGroup.hdel()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hdel()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.hdel()
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hdel()
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
        reply = hGroup.hdel()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        rhh.remove('field')
        100.times {
            rhh.put('field' + it, ' '.bytes)
        }
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hdel()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hdel()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hdel()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hexists'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'field'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hexists', data3, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hexists', data3, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.hexists()
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hexists()
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        reply = hGroup.hexists()
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
        reply = hGroup.hexists()
        then:
        reply == IntegerReply.REPLY_1

        when:
        data3[2] = 'field1'.bytes
        reply = hGroup.hexists()
        then:
        reply == IntegerReply.REPLY_0

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hexists()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hexists()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hget'() {
        given:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = 'field'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hget', data3, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hget', data3, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.hget(true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hget(true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.hget(false)
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hget(false)
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        reply = hGroup.hget(true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == cvField.compressedData.length

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hget(true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.hget(false)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == cvField.compressedData

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hget(false)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == ' '.bytes

        when:
        data3[2] = 'field1'.bytes
        reply = hGroup.hget(true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        reply = hGroup.hget(false)
        then:
        reply == NilReply.INSTANCE

        when:
        data3[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hget(true)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data3[1] = 'a'.bytes
        data3[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hget(true)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hgetall'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hgetall', data2, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hgetall', data2, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hgetall()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hgetall()
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
        reply = hGroup.hgetall()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'field'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == cvField.compressedData

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hgetall()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'field'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == ' '.bytes

        when:
        rhh.remove('field')
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hgetall()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        reply = hGroup.hgetall()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'field'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof NilReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.remove('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.hgetall()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hgetall()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hincrby'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hincrby', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hincrby', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.fieldKey('a', 'field'))
        def reply = hGroup.hincrby(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hincrby(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.hincrby(true)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '2.00'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hincrby(true)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '2.00'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[3] = 'a'.bytes
        reply = hGroup.hincrby(false)
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.hincrby(true)
        then:
        reply == ErrorReply.NOT_FLOAT

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hincrby(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hincrby(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = '1'.bytes
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', '0'.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hincrby(false)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        rhh.put('field', '1.1'.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hincrby(true)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '2.10'.bytes

        when:
        rhh.remove('field')
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hincrby(true)
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == '1.00'.bytes

        when:
        rhh.put('field', 'a'.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hincrby(true)
        then:
        reply == ErrorReply.NOT_FLOAT
    }

    def 'test hkeys'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hkeys', data2, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hkeys', data2, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hkeys(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hkeys(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.hkeys(true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hkeys(true)
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
        reply = hGroup.hkeys(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'field'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hkeys(false)
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'field'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.hkeys(true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hkeys(true)
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.remove('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.hkeys(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        rhh.remove('field')
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hkeys(false)
        then:
        reply == MultiBulkReply.EMPTY

        when:
        reply = hGroup.hkeys(true)
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hkeys(false)
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hmget'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = 'field1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hmget', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hmget', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hmget()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hmget()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvList = Mock.prepareCompressedValueList(2)
        def cvField = cvList[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        def cvField1 = cvList[1]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field1'), 0, cvField1)
        reply = hGroup.hmget()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == cvField.compressedData
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == cvField1.compressedData

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        rhh.put('field', ' '.bytes)
        rhh.put('field1', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hmget()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == ' '.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == ' '.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hmget()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hmget()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }

    def 'test hmset'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hmset', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hmset', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hmset()
        then:
        reply == OKReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hmset()
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
        reply = hGroup.hmset()
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
        reply = hGroup.hmset()
        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.remove('field0')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        def data6 = new byte[6][]
        data6[1] = 'a'.bytes
        data6[2] = 'field'.bytes
        data6[3] = 'value'.bytes
        data6[4] = 'field0'.bytes
        data6[5] = 'value0'.bytes
        hGroup.data = data6
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hmset', data6, hGroup.slotNumber)
        reply = hGroup.hmset()
        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        rhh.remove('field0')
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hmset()
        then:
        reply == ErrorReply.HASH_SIZE_TO_LONG

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        10.times {
            rhk.remove('field' + it)
        }
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.hmset()
        then:
        reply == OKReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        10.times {
            rhh.remove('field' + it)
        }
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hmset()
        then:
        reply == OKReply.INSTANCE

        // get and compare
        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = 'field0'.bytes
        hGroup.data = data4
        hGroup.cmd = 'hmget'
        reply = hGroup.hmget()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'value'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == 'value0'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hmget()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'value'.bytes
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[1]).raw == 'value0'.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.data = data4
        hGroup.cmd = 'hmset'
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hmset()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hmset()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[2] = 'field'.bytes
        data4[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = hGroup.hmset()
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test hrandfield'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = '1'.bytes
        data4[3] = '1'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hrandfield', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hrandfield', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hrandfield()
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hrandfield()
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[3] = 'withvalues'.bytes
        reply = hGroup.hrandfield()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hrandfield()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        data4[3] = '1'.bytes
        reply = hGroup.hrandfield()
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hrandfield()
        then:
        reply == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[3] = 'withvalues'.bytes
        reply = hGroup.hrandfield()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hrandfield()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        10.times {
            rhk.add('field' + it)
        }
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        data4[3] = '1'.bytes
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        10.times {
            rhh.put('field' + it, ' '.bytes)
        }
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        // > rhk size
        data4[3] = (rhk.size() + 1).toString().bytes
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == rhk.size()

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        data4[3] = (rhh.size() + 1).toString().bytes
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == rhh.size()

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[2] = '1'.bytes
        data4[3] = 'withvalues'.bytes
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((MultiBulkReply) reply).replies[1] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvList = Mock.prepareCompressedValueList(rhk.size())
        rhk.size().times {
            inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field' + it), 0, cvList[it])
        }
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[2] = '-1'.bytes
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((MultiBulkReply) reply).replies[1] instanceof BulkReply

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[2] = '5'.bytes
        data4[3] = '5'.bytes
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        data4[2] = '-5'.bytes
        data4[3] = '-5'.bytes
        reply = hGroup.hrandfield()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 5

        when:
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hrandfield()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = 'a'.bytes
        reply = hGroup.hrandfield()
        then:
        reply == ErrorReply.NOT_INTEGER
    }

    def 'test hsetnx'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'a'.bytes
        data4[2] = 'field'.bytes
        data4[3] = 'value'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hsetnx', data4, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hsetnx', data4, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hsetnx()
        then:
        reply == IntegerReply.REPLY_1

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hsetnx()
        then:
        reply == IntegerReply.REPLY_1

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        reply = hGroup.hsetnx()
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        reply = hGroup.hsetnx()
        then:
        reply == IntegerReply.REPLY_0

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        data4[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hsetnx()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[1] = 'a'.bytes
        data4[2] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hsetnx()
        then:
        reply == ErrorReply.KEY_TOO_LONG

        when:
        data4[2] = 'field'.bytes
        data4[3] = new byte[CompressedValue.VALUE_MAX_LENGTH + 1]
        reply = hGroup.hsetnx()
        then:
        reply == ErrorReply.VALUE_TOO_LONG
    }

    def 'test hvals'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def hGroup = new HGroup('hvals', data2, null)
        hGroup.byPassGetSet = inMemoryGetSet
        hGroup.from(BaseCommand.mockAGroup())

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        hGroup.slotWithKeyHashListParsed = _HGroup.parseSlots('hvals', data2, hGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = hGroup.hvals()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        inMemoryGetSet.remove(slot, 'a')
        reply = hGroup.hvals()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvKeys = Mock.prepareCompressedValueList(1)[0]
        cvKeys.dictSeqOrSpType = CompressedValue.SP_TYPE_HASH
        def rhk = new RedisHashKeys()
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.hvals()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        def cvRhh = Mock.prepareCompressedValueList(1)[0]
        cvRhh.dictSeqOrSpType = CompressedValue.SP_TYPE_HH
        def rhh = new RedisHH()
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hvals()
        then:
        reply == MultiBulkReply.EMPTY

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        rhk.add('field')
        cvKeys.compressedData = rhk.encode()
        inMemoryGetSet.put(slot, RedisHashKeys.keysKey('a'), 0, cvKeys)
        reply = hGroup.hvals()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] == NilReply.INSTANCE

        when:
        LocalPersist.instance.hashSaveMemberTogether = true
        rhh.put('field', ' '.bytes)
        cvRhh.compressedData = rhh.encode()
        inMemoryGetSet.put(slot, 'a', 0, cvRhh)
        reply = hGroup.hvals()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == ' '.bytes

        when:
        LocalPersist.instance.hashSaveMemberTogether = false
        def cvField = Mock.prepareCompressedValueList(1)[0]
        inMemoryGetSet.put(slot, RedisHashKeys.fieldKey('a', 'field'), 0, cvField)
        reply = hGroup.hvals()
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == cvField.compressedData

        when:
        data2[1] = new byte[CompressedValue.KEY_MAX_LENGTH + 1]
        reply = hGroup.hvals()
        then:
        reply == ErrorReply.KEY_TOO_LONG
    }
}
