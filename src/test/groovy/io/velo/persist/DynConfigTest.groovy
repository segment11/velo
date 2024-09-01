package io.velo.persist

import io.velo.MultiWorkerServer
import io.velo.SocketInspector
import io.velo.TrainSampleJob
import io.velo.monitor.BigKeyTopK
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
        then:
        config.afterUpdateCallback != null
        config.masterUuid == 1234L
        config.testKey == 1
        config.readonly
        !config.canRead
        !config.canWrite

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

        cleanup:
        tmpFile.delete()
        tmpFile2.delete()
    }
}
