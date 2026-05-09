package io.velo.persist

import com.fasterxml.jackson.databind.ObjectMapper
import io.velo.MultiWorkerServer
import io.velo.SocketInspector
import io.velo.TrainSampleJob
import io.velo.monitor.BigKeyTopK
import io.velo.type.RedisHashKeys
import io.velo.type.RedisList
import io.velo.type.RedisZSet
import spock.lang.Specification

class DynConfigTest extends Specification {
    final short slot = 0

    static File tmpFile = new File('/tmp/dyn-config.json')
    static File tmpFile2 = new File('/tmp/dyn-config2.json')

    def 'test all'() {
        given:
        if (tmpFile.exists()) {
            tmpFile.delete()
        }
        def oneSlot = new OneSlot(slot)
        def config = new DynConfig(slot, tmpFile, oneSlot)

        expect:
        config.masterUuid == null
        !config.readonly
        config.canRead
        config.canWrite
        !config.binlogOn
        config.testKey == 10

        when:
        config.masterUuid = 1234L
        then:
        config.masterUuid == 1234L

        when:
        config.testKey = 1
        then:
        config.testKey == 1

        when:
        config.binlogOn = false
        then:
        !config.binlogOn

        when:
        config.binlogOn = true
        then:
        config.binlogOn

        when:
        config.readonly = true
        config.canRead = false
        config.canWrite = false
        then:
        config.readonly
        !config.canRead
        !config.canWrite

        // reload from file
        when:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()
        config = new DynConfig(slot, tmpFile, oneSlot)
        config.update(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, 100)
        config.update(TrainSampleJob.KEY_IN_DYN_CONFIG, 'key:,xxx:')
        config.update(BigKeyTopK.KEY_IN_DYN_CONFIG, 100)
        config.update('type_zset_member_max_length', 255)
        config.update('type_set_member_max_length', 255)
        config.update('type_zset_max_size', 4096)
        config.update('type_hash_max_size', 4096)
        config.update('type_list_max_size', 4096)
        then:
        config.afterUpdateCallback != null
        config.masterUuid == 1234L
        config.testKey == 1
        config.readonly
        !config.canRead
        !config.canWrite
        config.getLongValue('yyy', 2) == 2

        when:
        config.readonly = false
        config.canRead = true
        config.canWrite = true
        then:
        !config.readonly
        config.canRead
        config.canWrite

        when:
        // reload from file again
        new DynConfig(slot, tmpFile, oneSlot)
        then:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.maxConnections == 100
        TrainSampleJob.keyPrefixOrSuffixGroupList == ['key:', 'xxx:']

        when:
        // invalid max_connections should be rejected, keeping previous value
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.setMaxConnections(50)
        config.update(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, 0)
        then:
        thrown(IllegalArgumentException)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.maxConnections == 50

        when:
        config.update(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, -1)
        then:
        thrown(IllegalArgumentException)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.maxConnections == 50

        cleanup:
        tmpFile.delete()
        tmpFile2.delete()
    }

    def 'test max connections validates before persist'() {
        given:
        if (tmpFile.exists()) {
            tmpFile.delete()
        }
        def oneSlot = new OneSlot(slot)
        def config = new DynConfig(slot, tmpFile, oneSlot)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = new SocketInspector()

        when:
        config.update(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, '100')

        then:
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.maxConnections == 100
        config.get(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG) == 100

        when:
        config.update(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG, '0')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG)
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.maxConnections == 100
        config.get(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG) == 100
        new ObjectMapper().readValue(tmpFile, HashMap)[SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG] == 100

        when:
        config = new DynConfig(slot, tmpFile, oneSlot)

        then:
        config.get(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG) == 100
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector.maxConnections == 100

        cleanup:
        tmpFile.delete()
        tmpFile2.delete()
    }

    def 'test unsupported config key is rejected before persist'() {
        given:
        if (tmpFile.exists()) {
            tmpFile.delete()
        }
        def oneSlot = new OneSlot(slot)
        def config = new DynConfig(slot, tmpFile, oneSlot)

        expect:
        DynConfig.isSupportedKey(SocketInspector.MAX_CONNECTIONS_KEY_IN_DYN_CONFIG)
        DynConfig.isSupportedKey(BigKeyTopK.KEY_IN_DYN_CONFIG)
        !DynConfig.isSupportedKey('unknown_key')

        when:
        config.update('unknown_key', 'value')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('unknown_key')
        config.get('unknown_key') == null
        !new ObjectMapper().readValue(tmpFile, HashMap).containsKey('unknown_key')

        cleanup:
        tmpFile.delete()
        tmpFile2.delete()
    }

    def 'test supported config items normalize and validate values'() {
        given:
        if (tmpFile.exists()) {
            tmpFile.delete()
        }
        def oneSlot = new OneSlot(slot)
        def config = new DynConfig(slot, tmpFile, oneSlot)

        when:
        config.update(TrainSampleJob.KEY_IN_DYN_CONFIG, 'key:,xxx:')
        config.update(BigKeyTopK.KEY_IN_DYN_CONFIG, '100')
        config.update('type_zset_member_max_length', '128')
        config.update('type_set_member_max_length', '129')
        config.update('type_zset_max_size', '512')
        config.update('type_hash_max_size', '513')
        config.update('type_list_max_size', '514')
        config.update('repl_connect_timeout_millis', '6000')

        then:
        TrainSampleJob.keyPrefixOrSuffixGroupList == ['key:', 'xxx:']
        oneSlot.bigKeyTopK != null
        config.get(BigKeyTopK.KEY_IN_DYN_CONFIG) == 100
        RedisZSet.ZSET_MEMBER_MAX_LENGTH == (short) 128
        RedisHashKeys.SET_MEMBER_MAX_LENGTH == (short) 129
        RedisZSet.ZSET_MAX_SIZE == (short) 512
        RedisHashKeys.HASH_MAX_SIZE == (short) 513
        RedisList.LIST_MAX_SIZE == (short) 514
        config.getLongValue('repl_connect_timeout_millis', 1L) == 6000L

        and:
        def json = new ObjectMapper().readValue(tmpFile, HashMap)
        json[BigKeyTopK.KEY_IN_DYN_CONFIG] == 100
        json['type_hash_max_size'] == 513
        json['repl_connect_timeout_millis'] == 6000L

        when:
        config.update('type_hash_max_size', '0')

        then:
        def e = thrown(IllegalArgumentException)
        e.message.contains('type_hash_max_size')
        RedisHashKeys.HASH_MAX_SIZE == (short) 513
        config.get('type_hash_max_size') == (short) 513
        new ObjectMapper().readValue(tmpFile, HashMap)['type_hash_max_size'] == 513

        cleanup:
        tmpFile.delete()
        tmpFile2.delete()
    }
}
