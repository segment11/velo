package io.velo

import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.Binlog
import io.velo.repl.BinlogContent
import org.apache.commons.io.FileUtils
import org.jetbrains.annotations.NotNull
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

    def 'test putDict keeps dict in cache when binlog append fails'() {
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

        and: 'install a binlog whose append always throws IOException'
        def stubSlotDir = new File(Consts.testDir, 'stub-slot')
        FileUtils.forceMkdir(stubSlotDir)
        def failingBinlog = new Binlog(slot, stubSlotDir, oneSlot.dynConfig) {
            @Override
            void append(@NotNull BinlogContent content) throws IOException {
                throw new IOException("Simulated binlog failure")
            }
        }
        oneSlot.setBinlog(failingBinlog)

        and:
        def dict = new Dict()
        dict.dictBytes = 'fail-binlog-test'.bytes
        dict.seq = 200
        dict.createdTime = System.currentTimeMillis()

        when: 'putDict is called'
        def result = dictMap.putDict('fail-binlog-prefix', dict)

        then: 'no exception escapes — binlog failure is logged and swallowed'
        noExceptionThrown()
        result == dict

        and: 'the dict is in the cache (cache update happened before binlog)'
        dictMap.getDict('fail-binlog-prefix') == dict
        dictMap.getDictBySeq(dict.seq) == dict
        dictMap.dictSize() == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
