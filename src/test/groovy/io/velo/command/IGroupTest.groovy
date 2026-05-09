package io.velo.command

import com.fasterxml.jackson.databind.ObjectMapper
import io.velo.BaseCommand
import io.velo.DictMap
import io.velo.MultiWorkerServer
import io.velo.Utils
import io.velo.dyn.CachedGroovyClassLoader
import io.velo.mock.InMemoryGetSet
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.reply.*
import io.velo.type.RedisHashKeys
import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions
import spock.lang.Specification

class IGroupTest extends Specification {
    final short slot = 0
    def _IGroup = new IGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes
        int slotNumber = 128

        expect:
        _IGroup.parseSlots(cmd, data2, slotNumber).size() == expectedSize

        where:
        cmd            | expectedSize
        'incr'         | 1
        'incrby'       | 1
        'incrbyfloat'  | 1
        'ixxx'         | 0
    }

    def 'test parse slot - insufficient data'() {
        given:
        def data1 = new byte[1][]
        int slotNumber = 128

        expect:
        _IGroup.parseSlots('incrby', data1, slotNumber).size() == 0
    }

    def 'test handle - format errors'() {
        given:
        def iGroup = new IGroup(null, null, null)
        iGroup.from(BaseCommand.mockAGroup())

        expect:
        iGroup.execute(input) == expected

        where:
        input         | expected
        'incr'        | ErrorReply.FORMAT
        'incrby'      | ErrorReply.FORMAT
        'incrbyfloat' | ErrorReply.FORMAT
        'ingest'      | ErrorReply.FORMAT
        'ingest_sst'  | ErrorReply.FORMAT
        'zzz'         | NilReply.INSTANCE
    }

    def 'test handle - incrby incrbyfloat info'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()
        def iGroup = new IGroup(null, null, null)
        iGroup.byPassGetSet = inMemoryGetSet
        iGroup.from(BaseCommand.mockAGroup())

        def loader = CachedGroovyClassLoader.instance
        def classpath = Utils.projectPath('/dyn/src')
        loader.init(GroovyClassLoader.getClass().classLoader, classpath, null)

        when:
        def data33 = new byte[3][]
        data33[1] = 'a'.bytes
        data33[2] = '1'.bytes
        iGroup.cmd = 'incrby'
        iGroup.data = data33
        iGroup.slotWithKeyHashListParsed = _IGroup.parseSlots('incrby', data33, iGroup.slotNumber)
        def reply = iGroup.handle()
        then:
        reply instanceof IntegerReply

        when:
        iGroup.cmd = 'incrbyfloat'
        reply = iGroup.handle()
        then:
        reply instanceof DoubleReply

        when:
        def data3 = new byte[3][]
        iGroup.cmd = 'info'
        iGroup.data = data3
        reply = iGroup.handle()
        then:
        reply instanceof BulkReply
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
        (reply as IntegerReply).integer == 1

        when:
        def data3 = new byte[3][]
        data3[1] = 'a'.bytes
        data3[2] = '1'.bytes
        iGroup.data = data3
        iGroup.cmd = 'incrby'
        reply = iGroup.handle()
        then:
        reply instanceof IntegerReply
        (reply as IntegerReply).integer == 2

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
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.testDir)
        MultiWorkerServer.STATIC_GLOBAL_V.slotWorkerThreadIds = [Thread.currentThread().threadId()]
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

    def 'test ingest_sst'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def iGroup = new IGroup('ingest_sst', null, null)
        iGroup.byPassGetSet = inMemoryGetSet
        iGroup.from(BaseCommand.mockAGroup())

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and:
        def dir = new File(Consts.persistDir, 'ingest_sst')
        dir.mkdirs()

        and:
        // create sst files using rocks db jni
        def db = RocksDB.open(new Options().setCreateIfMissing(true), dir.absolutePath)
        def count = 10000
        println 'prepare sst key value data, count: ' + count
        def writeBatch = new WriteBatch()
        for (int i = 0; i < count; i++) {
            // value is uuid
            def value = UUID.randomUUID().toString()
            writeBatch.put(('key:' + i).bytes, value.bytes)
        }
        db.write(new WriteOptions(), writeBatch)
        db.close()
        println 'prepare sst key value data done'

        when:
        def reply = iGroup.execute('ingest_sst dir=' + dir.absolutePath)
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.getResult() instanceof MultiBulkReply
        (((reply as AsyncReply).settablePromise.getResult() as MultiBulkReply).replies[0] as BulkReply).raw == "slot: 0 put: ${count} skip: 0".bytes

        when:
        // dir not exists
        reply = iGroup.execute('ingest_sst dir=/tmp/xxx_x')
        then:
        reply instanceof ErrorReply

        when:
        // file not exists
        new File('/tmp/xxx_x').mkdirs()
        reply = iGroup.execute('ingest_sst dir=/tmp/xxx_x')
        then:
        reply instanceof ErrorReply

        when:
        // invalid rocks db file
        new File('/tmp/xxx_x/test.text').text = 'xxx'
        reply = iGroup.execute('ingest_sst dir=/tmp/xxx_x')
        then:
        reply instanceof ErrorReply

        cleanup:
        new File('/tmp/xxx_x').deleteDir()
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
