package io.velo

import io.velo.persist.DynConfig
import io.velo.repl.Binlog
import org.apache.commons.io.FileUtils
import spock.lang.Specification

class DictMapTest extends Specification {
    def 'test all'() {
        given:
        FileUtils.forceMkdir(io.velo.persist.Consts.testDir)

        def dictFile = new File(io.velo.persist.Consts.testDir, 'dict-map.dat')
        if (dictFile.exists()) {
            dictFile.delete()
        }

        and:
        def dictMap = DictMap.instance
        dictMap.cleanUp()
        dictMap.initDictMap(io.velo.persist.Consts.testDir)

        and:
        final short slot = 0
        def dynConfig = new DynConfig(slot, io.velo.persist.DynConfigTest.tmpFile)
        def binlog = new Binlog(slot, io.velo.persist.Consts.slotDir, dynConfig)
        dictMap.binlog = binlog

        def dict = new Dict()
        dict.dictBytes = 'test'.bytes
        dict.seq = 1
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
        dictMap.binlog = null
        dictMap.putDict('test2', dict2)
        then:
        dictMap.dictSize() == 3
        dictMap.getDict('test').dictBytes == 'test'.bytes
        dictMap.getDict('test2').dictBytes == 'test2'.bytes
        dictMap.getDictBySeq(1).dictBytes == 'test'.bytes
        dictMap.getDictBySeq(dict.seq).dictBytes == 'test'.bytes
        dictMap.getDictBySeq(0).dictBytes == 'test2'.bytes

        when:
        dictMap.updateGlobalDictBytes(new byte[10])
        then:
        Dict.GLOBAL_ZSTD_DICT.hasDictBytes()

        when:
        // reload again
        dictMap.initDictMap(io.velo.persist.Consts.testDir)
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
        binlog.truncateAll()
        binlog.cleanUp()
        io.velo.persist.Consts.slotDir.deleteDir()
        io.velo.persist.Consts.testDir.deleteDir()
    }
}
