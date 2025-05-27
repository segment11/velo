package io.velo.repl

import io.velo.ConfForSlot
import io.velo.persist.*
import io.velo.repl.incremental.XWalV
import spock.lang.Specification

import java.nio.ByteBuffer

class BinlogTest extends Specification {
    final short slot = 0

    def 'test append'() {
        given:
        ConfForSlot.global.confRepl.binlogForReadCacheSegmentMaxCount = 2

        println Binlog.oneFileMaxSegmentCount()

        def bytesWithFileIndexAndOffset = new Binlog.BytesWithFileIndexAndOffset(new byte[10], 0, 0)
        println bytesWithFileIndexAndOffset
        def fileIndexAndOffset = new Binlog.FileIndexAndOffset(1, 1)
        println fileIndexAndOffset
        println fileIndexAndOffset.asReplOffset()

        expect:
        !fileIndexAndOffset.equals(null)
        !fileIndexAndOffset.equals(new Object())
        fileIndexAndOffset != bytesWithFileIndexAndOffset
        fileIndexAndOffset == new Binlog.FileIndexAndOffset(1, 1)
        fileIndexAndOffset != new Binlog.FileIndexAndOffset(1, 0)
        fileIndexAndOffset != new Binlog.FileIndexAndOffset(0, 1)
        new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 0) > new Binlog.BytesWithFileIndexAndOffset(new byte[10], 0, 0)
        new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 1) > new Binlog.BytesWithFileIndexAndOffset(new byte[10], 1, 0)
        Binlog.marginFileOffset(100) == 0
        Binlog.marginFileOffset(1024 * 1024 + 100) == 1024 * 1024

        when:
        def oneSlot = new OneSlot(slot)
        def dynConfig = new DynConfig(slot, DynConfigTest.tmpFile, oneSlot)
        def dynConfig2 = new DynConfig(slot, DynConfigTest.tmpFile2, oneSlot)
        dynConfig.binlogOn = true
        dynConfig2.binlogOn = false
        def binlog = new Binlog(slot, Consts.slotDir, dynConfig)
        println binlog
        println binlog.currentFileIndexAndOffset()
        println binlog.earliestFileIndexAndOffset()
        println binlog.currentReplOffset()
        println binlog.diskUsage
        println 'in memory size estimate: ' + binlog.estimate(new StringBuilder())

        final File slotDir2 = new File('/tmp/velo-data/test-persist/test-slot2')
        if (!slotDir2.exists()) {
            slotDir2.mkdir()
            def binlogDir2 = new File(slotDir2, 'binlog')
            if (!binlogDir2.exists()) {
                binlogDir2.mkdir()
            }
            new File(binlogDir2, 'test.txt').text = 'test'
        }
        def binlog2 = new Binlog(slot, slotDir2, dynConfig2)
        and:
        def vList = Mock.prepareValueList(11)
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
            binlog2.append(new XWalV(v))
        }
        then:
        1 == 1

        when:
        boolean exception = false
        try {
            binlog.readPrevRafOneSegment(1, 0)
        } catch (IOException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            binlog.readPrevRafOneSegment(-1, 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            binlog.prevRaf(-1) == null
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength
        def oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        binlog.resetCurrentFileOffset oneFileMaxLength - 1
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        then:
        binlog.currentFileIndex == 1
        binlog.prevRaf(0) != null
        binlog.readPrevRafOneSegment(0, 0).length == oneSegmentLength
        binlog.readPrevRafOneSegment(1, 0).length < oneSegmentLength

        when:
        binlog.resetCurrentFileOffset oneSegmentLength - 1
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        binlog.resetCurrentFileOffset oneSegmentLength * 2 - 1
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        binlog.resetCurrentFileOffset oneSegmentLength * 3 - 1
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        then:
        binlog.readCurrentRafOneSegment(0).length == oneSegmentLength
        binlog.readCurrentRafOneSegment(oneSegmentLength).length == oneSegmentLength
        binlog.readCurrentRafOneSegment(oneSegmentLength * 2).length == oneSegmentLength
        binlog.readCurrentRafOneSegment(oneSegmentLength * 3).length == oneSegmentLength
        binlog.readCurrentRafOneSegment(binlog.currentFileOffset) == null

        when:
        def lastAppendFileOffset = binlog.currentFileOffset
        binlog.resetCurrentFileOffset oneSegmentLength * 4
        then:
        binlog.readCurrentRafOneSegment(oneSegmentLength * 3).length > 0

        when:
        // for cache
        def bytes = binlog.readPrevRafOneSegment(binlog.currentFileIndex, oneSegmentLength)
        then:
        bytes != null

        when:
        bytes = binlog.readPrevRafOneSegment(binlog.currentFileIndex, oneSegmentLength * 10)
        then:
        bytes == null

        when:
        exception = false
        try {
            binlog.readPrevRafOneSegment(0, 1)
        } catch (IllegalArgumentException ignored) {
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            binlog.readCurrentRafOneSegment(1)
        } catch (IllegalArgumentException ignored) {
            exception = true
        }
        then:
        exception

        when:
        // current file index == 1
        def oldCurrentFileIndex = binlog.currentFileIndex
        binlog.cleanUp()
        // load again
        binlog = new Binlog(slot, Consts.slotDir, dynConfig)
        then:
        binlog.currentFileIndex == oldCurrentFileIndex
        binlog.currentFileOffset == lastAppendFileOffset
        binlog.prevRaf(0) != null

        when:
        ConfForSlot.global.confRepl.binlogFileKeepMaxCount = 1
        binlog.resetCurrentFileOffset oneFileMaxLength - 1
        // trigger remove old file
        for (v in vList[0..9]) {
            binlog.append(new XWalV(v))
        }
        then:
        binlog.currentFileIndex == 2

        when:
        binlog.resetCurrentFileOffset oneSegmentLength
        def xWalV = new XWalV(vList[0])
        binlog.append(xWalV)
        then:
        binlog.currentFileOffset == oneSegmentLength + xWalV.encodedLength()

        when:
        exception = false
        def testBinlogContent = new BinlogContent() {
            @Override
            BinlogContent.Type type() {
                return null
            }

            @Override
            int encodedLength() {
                return 0
            }

            @Override
            byte[] encodeWithType() {
                new byte[oneSegmentLength]
            }

            @Override
            void apply(short slot, ReplPair replPair) {

            }
        }
        try {
            binlog.append(testBinlogContent)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        // write from master
        when:
        def oneSegmentBytes = new byte[oneSegmentLength]
        // write to current file index
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 2, 0)
        then:
        binlog.currentFileOffset == oneSegmentLength

        when:
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 2, oneFileMaxLength - oneSegmentLength)
        then:
        // create next file
        binlog.currentFileIndex == 3
        binlog.currentFileOffset == 0

        when:
        // write perv again
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 2, 0)
        then:
        // not change
        binlog.currentFileIndex == 3
        binlog.currentFileOffset == 0

        when:
        // write to next file not created yet
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 4, 0)
        then:
        // change to bigger file index
        binlog.currentFileIndex == 4
        binlog.currentFileOffset == oneSegmentLength

        when:
        // write to next file not created yet
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 5, oneFileMaxLength - oneSegmentLength)
        then:
        // create next file
        binlog.currentFileIndex == 6
        binlog.currentFileOffset == 0

        when:
        oneSegmentBytes = new byte[oneSegmentLength / 2]
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 5, oneFileMaxLength - oneSegmentLength)
        then:
        // not change as current file index is bigger
        binlog.currentFileIndex == 6
        binlog.currentFileOffset == 0

        when:
        // test padding binlog content
        def paddingBinlogContent = new Binlog.PaddingBinlogContent(new byte[10])
        paddingBinlogContent.apply(slot, null)
        then:
        paddingBinlogContent.type() == null
        paddingBinlogContent.encodedLength() == 10
        paddingBinlogContent.encodeWithType().length == 10

        when:
        binlog.moveToNextSegment(true)
        then:
        binlog.currentFileIndex == 6
        binlog.currentFileOffset == oneSegmentLength

        when:
        binlog.resetCurrentFileOffset oneSegmentLength + 1
        binlog.moveToNextSegment()
        then:
        binlog.currentFileIndex == 6
        binlog.currentFileOffset == oneSegmentLength * 2

        when:
        binlog.resetCurrentFileOffset oneFileMaxLength - oneSegmentLength + 1
        binlog.moveToNextSegment()
        then:
        binlog.currentFileIndex == 7
        binlog.currentFileOffset == 0

        when:
        binlog.moveToNextSegment(false)
        then:
        binlog.currentFileIndex == 7
        binlog.currentFileOffset == 0

        when:
        binlog.moveToNextSegment(true)
        then:
        binlog.currentFileIndex == 7
        binlog.currentFileOffset == oneSegmentLength

        when:
        binlog.resetCurrentFileOffset oneFileMaxLength - oneSegmentLength
        binlog.moveToNextSegment(true)
        then:
        binlog.currentFileIndex == 8
        binlog.currentFileOffset == 0

        when:
        binlog.reopenAtFileIndexAndMarginOffset(9, 0)
        then:
        binlog.currentFileIndex == 9
        binlog.currentFileOffset == 0

        when:
        binlog.reopenAtFileIndexAndMarginOffset(9, oneSegmentLength)
        then:
        binlog.currentFileIndex == 9
        binlog.currentFileOffset == oneSegmentLength

        when:
        binlog.reopenAtFileIndexAndMarginOffset(9, 0)
        then:
        binlog.currentFileIndex == 9
        binlog.currentFileOffset == 0

        when:
        exception = false
        oneSegmentBytes = new byte[oneSegmentLength + 1]
        try {
            binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 5, 0)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        dynConfig.binlogOn = false
        binlog.writeFromMasterOneSegmentBytes(oneSegmentBytes, 5, 0)
        then:
        1 == 1

        cleanup:
        println 'in memory size estimate: ' + binlog.estimate(new StringBuilder())
        binlog.truncateAll()
        binlog.cleanUp()
        Consts.slotDir.deleteDir()
        slotDir2.deleteDir()
    }

    def 'test apply'() {
        given:
        def oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        def oneSegmentBytes = new byte[oneSegmentLength]
        def buffer = ByteBuffer.wrap(oneSegmentBytes)

        and:
        def vList = Mock.prepareValueList(10)
        vList.each { v ->
            def xWalV = new XWalV(v)
            def encoded = xWalV.encodeWithType()
            buffer.put(encoded)
        }

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def replPair = ReplPairTest.mockAsSlave()
        Binlog.decodeAndApply(slot, oneSegmentBytes, 0, replPair)
        then:
        oneSlot.getWalByBucketIndex(0).keyCount == 10

        when:
        def n = Binlog.decodeAndApply(slot, oneSegmentBytes, oneSegmentBytes.length, replPair)
        then:
        n == 0

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
