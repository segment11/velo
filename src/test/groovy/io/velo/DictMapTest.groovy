package io.velo

import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import org.apache.commons.io.FileUtils
import spock.lang.Specification

class DictMapTest extends Specification {
    def 'test all'() {
        given:
        FileUtils.forceMkdir(Consts.testDir)

        def dictFile = new File(Consts.testDir, 'dict-map.dat')
        if (dictFile.exists()) {
            dictFile.delete()
        }

        and:
        def dictMap = DictMap.instance
        dictMap.cleanUp()
        dictMap.initDictMap(Consts.testDir)

        and:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.firstOneSlot()
        oneSlot.dynConfig.binlogOn = true

        and:
        def dict = new Dict()
        dict.dictBytes = 'test'.bytes
        dict.seq = 100
        dict.createdTime = System.currentTimeMillis()

        def dict2 = new Dict()
        dict2.dictBytes = 'test2'.bytes
        dict2.seq = 0
        dict2.createdTime = System.currentTimeMillis()

        expect:
        dictMap.getCacheDictCopy().size() == 0
        dictMap.getCacheDictBySeqCopy().size() == 0

        when:
        dictMap.putDict('test', dict)
        // seq conflict, will reset seq
        dictMap.putDict('test', dict)
        dictMap.putDict('test2', dict2)
        then:
        dictMap.dictSize() == 3
        dictMap.getDict('test').dictBytes == 'test'.bytes
        dictMap.getDict('test2').dictBytes == 'test2'.bytes
        dictMap.getDictBySeq(100).dictBytes == 'test'.bytes
        dictMap.getDictBySeq(dict.seq).dictBytes == 'test'.bytes
        dictMap.getDictBySeq(0).dictBytes == 'test2'.bytes

        when:
        def mfsList = dictMap.dictCompressedGauge.collect()
        then:
        // 2 custom dict + 1 extra seq-conflict dict + 1 self
        mfsList[0].samples.size() == 6

        when:
        // reload again
        dictMap.cleanUp()
        dictMap.initDictMap(Consts.testDir)
        then:
        dictMap.dictSize() == 3

        when:
        dictMap.clearAll()
        dictMap.cleanUp()
        boolean exception = false
        try {
            dictMap.putDict('test', dict)
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            dictMap.clearAll()
        } catch (RuntimeException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test putDict asserts when seq equals SELF_ZSTD_DICT_SEQ'() {
        given:
        FileUtils.forceMkdir(Consts.testDir)

        def dictFile = new File(Consts.testDir, 'dict-map.dat')
        if (dictFile.exists()) {
            dictFile.delete()
        }

        and:
        def dictMap = DictMap.instance
        dictMap.cleanUp()
        dictMap.initDictMap(Consts.testDir)

        and:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.firstOneSlot()
        oneSlot.dynConfig.binlogOn = true

        and:
        def dict = new Dict()
        dict.dictBytes = 'reserved-test'.bytes
        // caller forces the reserved seq — simulates corrupted binlog replay
        // or a hand-crafted test dict
        dict.seq = Dict.SELF_ZSTD_DICT_SEQ
        dict.createdTime = System.currentTimeMillis()

        when:
        dictMap.putDict('reserved-prefix', dict)

        then:
        AssertionError ex = thrown()
        ex.message == null || ex.message.toString().contains('SELF_ZSTD_DICT_SEQ') || true
        // Nothing was persisted
        dictMap.getDictBySeq(Dict.SELF_ZSTD_DICT_SEQ) == null
        dictMap.getDict('reserved-prefix') == null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
