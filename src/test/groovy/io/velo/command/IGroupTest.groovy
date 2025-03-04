package io.velo.command

import com.fasterxml.jackson.databind.ObjectMapper
import io.velo.BaseCommand
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.reply.*
import io.velo.type.RedisHashKeys
import spock.lang.Specification

class IGroupTest extends Specification {
    final short slot = 0
    def _IGroup = new IGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sIncrList = _IGroup.parseSlots('incr', data2, slotNumber)
        def sIncrbyList = _IGroup.parseSlots('incrby', data2, slotNumber)
        def sIncrbyfloatList = _IGroup.parseSlots('incrbyfloat', data2, slotNumber)
        def sList = _IGroup.parseSlots('ixxx', data2, slotNumber)
        then:
        sIncrList.size() == 1
        sIncrbyList.size() == 1
        sIncrbyfloatList.size() == 1
        sList.size() == 0

        when:
        def data1 = new byte[1][]

        sIncrbyList = _IGroup.parseSlots('incrby', data1, slotNumber)
        then:
        sIncrbyList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def iGroup = new IGroup('incr', data1, null)
        iGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = iGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'incrby'
        reply = iGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'incrbyfloat'
        reply = iGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        def data3 = new byte[3][]
        iGroup.cmd = 'info'
        iGroup.data = data3
        reply = iGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        iGroup.cmd = 'zzz'
        reply = iGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test handle2'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        def inMemoryGetSet = new InMemoryGetSet()

        def iGroup = new IGroup('incr', data2, null)
        iGroup.byPassGetSet = inMemoryGetSet
        iGroup.from(BaseCommand.mockAGroup())

        when:
        iGroup.slotWithKeyHashListParsed = _IGroup.parseSlots('incr', data2, iGroup.slotNumber)
        inMemoryGetSet.remove(slot, RedisHashKeys.keysKey('a'))
        def reply = iGroup.handle()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes
        iGroup.data = data3
        iGroup.cmd = 'incrby'
        reply = iGroup.handle()
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 2

        when:
        data3[2] = 'a'.bytes
        reply = iGroup.handle()
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        iGroup.cmd = 'incrbyfloat'
        reply = iGroup.handle()
        then:
        reply == ErrorReply.NOT_FLOAT
    }

    def 'test ingest'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def iGroup = new IGroup('ingest', null, null)
        iGroup.byPassGetSet = inMemoryGetSet
        iGroup.from(BaseCommand.mockAGroup())

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and:
        def dir = new File(Consts.persistDir, 'ingest')
        dir.mkdirs()

        when:
        new File(dir, 'test.csv').withPrintWriter { w ->
            100.times {
                w.println("$it,aaa,bbb")
            }
        }
        def reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.csv file_format=csv ' +
                'key_field_index=0 key_prefix=test_ value_fields_indexes=0,1,2')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() instanceof MultiBulkReply
        (((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[0] as BulkReply).raw == "slot: 0 put: 100 skip: 0".bytes

        when:
        // not all fields
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.csv file_format=csv ' +
                'key_field_index=0 key_prefix=test_ value_fields_indexes=0,1')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() instanceof MultiBulkReply
        (((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[0] as BulkReply).raw == "slot: 0 put: 100 skip: 0".bytes

        when:
        // key index over size
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.csv file_format=csv ' +
                'key_field_index=3 key_prefix=test_ value_fields_indexes=0,1')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getException() != null

        when:
        // json format
        new File(dir, 'test.json').withPrintWriter { w ->
            def objectMapper = new ObjectMapper()
            100.times {
                def map = [:]
                map.name = 'name' + it
                map.age = 20
                map.address = 'address' + it

                w.println(objectMapper.writeValueAsString(map))
            }
        }
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.json file_format=json ' +
                'key_field=name key_prefix=test_ value_fields=name,age,address')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() instanceof MultiBulkReply
        (((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[0] as BulkReply).raw == "slot: 0 put: 100 skip: 0".bytes

        when:
        // not all fields
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.json file_format=json ' +
                'key_field=name key_prefix=test_ value_fields=name,age')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() instanceof MultiBulkReply
        (((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[0] as BulkReply).raw == "slot: 0 put: 100 skip: 0".bytes

        when:
        // key field not exists
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.json file_format=json ' +
                'key_field=name1 key_prefix=test_ value_fields=name,age')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getException() != null

        when:
        // parquet format
        new File(dir, 'test.parquet').bytes = new File('src/test/groovy/sample.parquet').bytes
        new File(dir, 'test.schema').bytes = new File('src/test/groovy/sample.schema').bytes
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.parquet file_format=parquet ' +
                'key_field=id key_prefix=test_ value_fields=id,name,score,des schema_file=test.schema')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() instanceof MultiBulkReply
        (((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[0] as BulkReply).raw == "slot: 0 put: 300 skip: 0".bytes

        when:
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.parquet file_format=parquet ' +
                'key_field=id key_prefix=test_ value_fields=id,name,score schema_file=test.schema')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() instanceof MultiBulkReply
        (((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[0] as BulkReply).raw == "slot: 0 put: 300 skip: 0".bytes

        when:
        // dir not exists
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest2 file_pattern=test.json file_format=json ' +
                'key_field=name key_prefix=test_ value_fields=name,age')
        then:
        reply instanceof ErrorReply

        when:
        // file not exists
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test2.json file_format=json ' +
                'key_field=name key_prefix=test_ value_fields=name,age')
        then:
        reply instanceof ErrorReply

        when:
        // schema file not exists
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.parquet file_format=parquet ' +
                'key_field=name key_prefix=test_ value_fields=name,age schema_file=test2.schema')
        then:
        reply instanceof ErrorReply

        when:
        // key field not exists
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.parquet file_format=parquet ' +
                'key_field=name1 key_prefix=test_ value_fields=name,age schema_file=test.schema')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getException() != null

        when:
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.csv file_format=csv ' +
                'key_field_index=a key_prefix=test_ value_fields_indexes=0,1,2')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.csv file_format=csv ' +
                'key_field_index=0 key_prefix=test_ value_fields_indexes=0,1,a')
        then:
        reply == ErrorReply.NOT_INTEGER

        when:
        // data length not match
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.parquet file_format=parquet ' +
                'key_field=name1 key_prefix=test_ value_fields=name,age')
        then:
        reply == ErrorReply.FORMAT

        when:
        reply = iGroup.execute('ingest dir=/tmp/velo-data/test-persist/ingest file_pattern=test.json file_format=json')
        then:
        reply == ErrorReply.FORMAT

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
