package io.velo.persist

import io.velo.CompressedValue
import io.velo.KeyHash
import org.apache.commons.io.FileUtils
import spock.lang.Specification

import java.nio.file.Files
import java.nio.file.attribute.PosixFilePermissions

class BigStringFilesTest extends Specification {
    final short slot = 0

    def 'test write and read'() {
        given:
        def bigString = 'a' * 10000

        println new BigStringFiles.Id(1234L, 0)
        println new BigStringFiles.IdWithKey(1234L, 0, 1234L, '1234')

        def tmpSlotDir1 = new File('/tmp/tmp-slot-dir')
        def tmpSlotDir2 = new File('/tmp/tmp-slot-dir2')
        if (tmpSlotDir1.exists()) {
            tmpSlotDir1.deleteDir()
        }
        if (!tmpSlotDir2.exists()) {
            tmpSlotDir2.mkdirs()
        }

        def bigStringFiles1 = new BigStringFiles(slot, tmpSlotDir1)
        def bigStringFiles11 = new BigStringFiles(slot, tmpSlotDir1)
        def bigStringFiles2 = new BigStringFiles(slot, tmpSlotDir2)
        println bigStringFiles1.estimate(new StringBuilder())
        bigStringFiles1.collect()

        when:
        def isWriteOk = bigStringFiles1.writeBigStringBytes(1L, 0, 1L, bigString.bytes)
        def isWriteOk2 = bigStringFiles1.writeBigStringBytes(1L, 0, 1L, bigString.bytes)
        then:
        isWriteOk
        isWriteOk2
        bigStringFiles1.bigStringFilesCount == 1
        bigStringFiles1.getBigStringBytes(1L, 1, 1L) == null
        bigStringFiles1.getBigStringBytes(1L, 0, 1L) == bigString.bytes
        bigStringFiles1.getBigStringBytes(1L, 0, 1L, true) == bigString.bytes
        bigStringFiles1.getBigStringBytes(1L, 0, 1L, true) == bigString.bytes
        bigStringFiles1.getBigStringFileIdList(0).size() == 1
        bigStringFiles11.getBigStringFileIdList(0).size() == 1
        bigStringFiles2.getBigStringBytes(1L, 0, 1L) == null

        when:
        def bigStringFiles111 = new BigStringFiles(slot, tmpSlotDir1)
        then:
        bigStringFiles111.bucketIndexesWhenFirstServerStart.size() == 1

        when:
        bigStringFiles1.deleteBigStringFileIfExist(1L, 0, 1L)
        bigStringFiles2.deleteBigStringFileIfExist(1L, 0, 1L)
        bigStringFiles1.deleteAllBigStringFiles()
        bigStringFiles2.deleteAllBigStringFiles()
        then:
        bigStringFiles1.bigStringFilesCount == 0
        bigStringFiles1.getBigStringFileIdList(0).size() == 0
        bigStringFiles2.getBigStringFileIdList(0).size() == 0
    }

    def 'test write io exception'() {
        given:
        def noPermitDir = new File('/usr/tmp-slot-dir')

        when:
        boolean exception = false
        try {
            new BigStringFiles(slot, noPermitDir)
        } catch (IOException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        def permitDir = new File('/tmp/tmp-slot-dir-x')
        def bigStringFiles = new BigStringFiles(slot, permitDir)
        def bigStringDir = new File(permitDir, 'big-string')
        bigStringDir.mkdirs()

        def targetFile = new File(bigStringDir, '1')
        FileUtils.touch(targetFile)

        Files.setAttribute(bigStringDir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('r--r--r--'))

        def bigString = 'a' * 10000
        def isWriteOk = bigStringFiles.writeBigStringBytes(1L, 0, 1L, bigString.bytes)

        then:
        !isWriteOk

        cleanup:
        // delete dir
        Files.setAttribute(bigStringDir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('rwxrwxrwx'))
        bigStringDir.deleteDir()
    }

    def 'test read io exception'() {
        given:
        def permitDir = new File('/tmp/tmp-slot-dir-x2')
        def bigStringFiles = new BigStringFiles(slot, permitDir)
        def bigStringDir = new File(permitDir, 'big-string')
        bigStringDir.mkdirs()

        def targetFile = new File(bigStringDir, '1')
        FileUtils.touch(targetFile)

        when:
        Files.setAttribute(targetFile.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('-w--w--w-'))

        then:
        bigStringFiles.getBigStringBytes(1L, 0, 1L) == null

        cleanup:
        // delete dir
        Files.setAttribute(bigStringDir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('rwxrwxrwx'))
        bigStringDir.deleteDir()
    }

    def 'test sliced write accounting uses actual length'() {
        given:
        def tmpSlotDir = new File('/tmp/tmp-slot-dir-slice')
        if (tmpSlotDir.exists()) {
            tmpSlotDir.deleteDir()
        }
        def bigStringFiles = new BigStringFiles(slot, tmpSlotDir)
        def bytes = '0123456789'.bytes

        when:
        def isWriteOk = bigStringFiles.writeBigStringBytes(2L, 0, 2L, bytes, 2, 4)

        then:
        isWriteOk
        bigStringFiles.getBigStringBytes(2L, 0, 2L) == '2345'.bytes
        bigStringFiles.diskUsage == 4
        bigStringFiles.writeByteLengthTotal == 4

        cleanup:
        tmpSlotDir.deleteDir()
    }

    def 'test 3-arg getBigStringBytes seeds LRU after disk read'() {
        given:
        def tmpSlotDir = new File('/tmp/tmp-slot-dir-3arg-lru')
        if (tmpSlotDir.exists()) {
            tmpSlotDir.deleteDir()
        }
        def bigStringFiles = new BigStringFiles(slot, tmpSlotDir)
        def bytes = 'hello big string'.bytes
        bigStringFiles.writeBigStringBytes(10L, 0, 100L, bytes)
        bigStringFiles.clearLRUCache()

        when: '3-arg overload reads from disk'
        def result = bigStringFiles.getBigStringBytes(10L, 0, 100L)
        then:
        result == bytes

        when: 'read again - should be served from LRU (seeded by the 3-arg call above)'
        bigStringFiles.clearLRUCache()
        def resultFromCache = bigStringFiles.getBigStringBytes(10L, 0, 100L, true)
        then:
        resultFromCache == bytes

        when: '3-arg overload seeds LRU, verify LRU is populated after disk read'
        bigStringFiles.clearLRUCache()
        bigStringFiles.readFileCountTotal = 0
        // first call reads from disk
        bigStringFiles.getBigStringBytes(10L, 0, 100L)
        def readCountAfterFirst = bigStringFiles.readFileCountTotal
        // second call should hit LRU, not disk
        bigStringFiles.getBigStringBytes(10L, 0, 100L)
        def readCountAfterSecond = bigStringFiles.readFileCountTotal
        then:
        readCountAfterFirst == 1
        readCountAfterSecond == 1

        cleanup:
        tmpSlotDir.deleteDir()
    }

    def 'test 4-arg getBigStringBytes with doLRUCache=false bypasses LRU'() {
        given:
        def tmpSlotDir = new File('/tmp/tmp-slot-dir-nocache')
        if (tmpSlotDir.exists()) {
            tmpSlotDir.deleteDir()
        }
        def bigStringFiles = new BigStringFiles(slot, tmpSlotDir)
        def bytes = 'data without cache'.bytes
        bigStringFiles.writeBigStringBytes(20L, 0, 200L, bytes)

        when: 'read with doLRUCache=false — should return data but not seed LRU'
        bigStringFiles.readFileCountTotal = 0
        def result = bigStringFiles.getBigStringBytes(20L, 0, 200L, false)
        then:
        result == bytes
        bigStringFiles.readFileCountTotal == 1

        when: 'read again with doLRUCache=true — LRU was not seeded, so reads from disk again'
        def result2 = bigStringFiles.getBigStringBytes(20L, 0, 200L, true)
        then:
        result2 == bytes
        bigStringFiles.readFileCountTotal == 2

        when: 'read again with doLRUCache=true — now LRU is seeded'
        def result3 = bigStringFiles.getBigStringBytes(20L, 0, 200L, true)
        then:
        result3 == bytes
        bigStringFiles.readFileCountTotal == 2

        cleanup:
        tmpSlotDir.deleteDir()
    }

    def 'test failed delete keeps accounting unchanged'() {
        given:
        def tmpSlotDir = new File('/tmp/tmp-slot-dir-delete-fail')
        if (tmpSlotDir.exists()) {
            tmpSlotDir.deleteDir()
        }
        def bigStringFiles = new BigStringFiles(slot, tmpSlotDir)
        def file = new File(bigStringFiles.bigStringDir, '0/3_3')
        file.parentFile.mkdirs()
        file.bytes = 'abcd'.bytes
        bigStringFiles.bigStringFilesCount = 1
        bigStringFiles.diskUsage = 4
        def dir = file.parentFile

        when:
        Files.setAttribute(dir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('r-xr-xr-x'))
        def isDeleted = bigStringFiles.deleteBigStringFileIfExist(3L, 0, 3L)

        then:
        !isDeleted
        bigStringFiles.bigStringFilesCount == 1
        bigStringFiles.diskUsage == 4
        bigStringFiles.deleteFileCountTotal == 0
        bigStringFiles.deleteByteLengthTotal == 0

        cleanup:
        Files.setAttribute(dir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('rwxrwxrwx'))
        tmpSlotDir.deleteDir()
    }

    def 'test uuid map update from encoded'() {
        given:
        def tmpSlotDir = new File('/tmp/tmp-slot-dir-uuid-map')
        tmpSlotDir.mkdirs()
        def bigStringFiles = new BigStringFiles(slot, tmpSlotDir)

        // prepare big string encoded bytes
        def cvBig = new CompressedValue()
        cvBig.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cvBig.setCompressedDataAsBigString(8888L, CompressedValue.NULL_DICT_SEQ)
        def bigEncoded = cvBig.encode()
        println 'big encoded length: ' + bigEncoded.length

        // prepare normal short string encoded bytes
        def shortEncoded = Mock.prepareShortStringCvEncoded('key1', 'val1')

        expect:
        bigStringFiles.bigStringUuidByKey.isEmpty()

        when: 'big string encoded -> put uuid'
        bigStringFiles.updateUuidFromEncoded('key-a', bigEncoded)
        then:
        bigStringFiles.getBigStringUuid('key-a') == 8888L
        bigStringFiles.containsUuid(8888L)
        bigStringFiles.uuidMapSize() == 1

        when: 'non-big-string encoded -> remove'
        bigStringFiles.updateUuidFromEncoded('key-a', shortEncoded)
        then:
        bigStringFiles.getBigStringUuid('key-a') == null
        bigStringFiles.uuidMapSize() == 0

        when: 'short bytes (<28) -> remove (tombstone)'
        bigStringFiles.updateUuidFromEncoded('key-a', bigEncoded)
        bigStringFiles.updateUuidFromEncoded('key-a', [CompressedValue.SP_FLAG_DELETE_TMP] as byte[])
        then:
        bigStringFiles.getBigStringUuid('key-a') == null

        when: 'removeBigStringUuid'
        bigStringFiles.updateUuidFromEncoded('key-b', bigEncoded)
        bigStringFiles.removeBigStringUuid('key-b')
        then:
        bigStringFiles.getBigStringUuid('key-b') == null

        when: 'clearUuidMap'
        bigStringFiles.updateUuidFromEncoded('key-c', bigEncoded)
        bigStringFiles.clearUuidMap()
        then:
        bigStringFiles.uuidMapSize() == 0

        cleanup:
        bigStringFiles.deleteAllBigStringFiles()
        tmpSlotDir.deleteDir()
    }

    def 'test handleWhenCvExpiredOrDeleted only removes matching uuid'() {
        given:
        def tmpSlotDir = new File('/tmp/tmp-slot-dir-uuid-expire')
        tmpSlotDir.mkdirs()
        def bigStringFiles = new BigStringFiles(slot, tmpSlotDir)

        def key = 'expire-key'
        def oldUuid = 1111L
        def newUuid = 2222L
        // use keyHash=0 so bucketIndex=0 (bucketIndex = keyHash & (bucketsPerSlot-1))
        def keyHash = 0L

        // write old big string file to bucket 0
        bigStringFiles.writeBigStringBytes(oldUuid, 0, keyHash, 'old'.bytes)
        bigStringFiles.writeBigStringBytes(newUuid, 0, keyHash, 'new'.bytes)

        // map has new UUID (simulating an overwrite)
        def cvNew2 = new CompressedValue()
        cvNew2.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cvNew2.setCompressedDataAsBigString(newUuid, CompressedValue.NULL_DICT_SEQ)
        bigStringFiles.updateUuidFromEncoded(key, cvNew2.encode())

        when: 'expire callback with old UUID -> should not remove new UUID from map'
        def cvOld = new CompressedValue()
        cvOld.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cvOld.keyHash = keyHash
        cvOld.setCompressedDataAsBigString(oldUuid, CompressedValue.NULL_DICT_SEQ)
        bigStringFiles.handleWhenCvExpiredOrDeleted(key, cvOld, null)

        then: 'new UUID still in map, old file deleted'
        bigStringFiles.getBigStringUuid(key) == newUuid
        !new File(tmpSlotDir, 'big-string/0/' + oldUuid + '_' + keyHash).exists()
        new File(tmpSlotDir, 'big-string/0/' + newUuid + '_' + keyHash).exists()

        cleanup:
        bigStringFiles.deleteAllBigStringFiles()
        tmpSlotDir.deleteDir()
    }

    def 'test populateFromKeyBuckets fills uuid map from persisted entries'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        def bigStringFiles = oneSlot.bigStringFiles
        def keyLoader = oneSlot.keyLoader
        bigStringFiles.clearUuidMap()

        // write a big string entry directly to key bucket
        def key = 'bs-populate-key'
        def keyHash = KeyHash.hash(key.bytes)
        def cv = new CompressedValue()
        cv.seq = 100L
        cv.keyHash = keyHash
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.setCompressedDataAsBigString(9999L, CompressedValue.NULL_DICT_SEQ)
        keyLoader.putValueByKey(0, key, keyHash, 0L, 100L, cv.encode())

        when: 'populate from key buckets'
        bigStringFiles.populateFromKeyBuckets(keyLoader, 0)

        then: 'uuid map has the key'
        bigStringFiles.getBigStringUuid(key) == 9999L

        when: 'pre-populate via WAL with a different UUID, then populate from key buckets'
        def walCv = new CompressedValue()
        walCv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        walCv.setCompressedDataAsBigString(8888L, CompressedValue.NULL_DICT_SEQ)
        bigStringFiles.updateUuidFromEncoded(key, walCv.encode())
        bigStringFiles.populateFromKeyBuckets(keyLoader, 0)

        then: 'WAL entry takes precedence over key bucket'
        bigStringFiles.getBigStringUuid(key) == 8888L

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test getBigStringFileIdList skips unparseable stray files without throwing'() {
        given:
        def tmpSlotDir = new File('/tmp/tmp-slot-dir-stray-files')
        if (tmpSlotDir.exists()) {
            tmpSlotDir.deleteDir()
        }
        def bigStringFiles = new BigStringFiles(slot, tmpSlotDir)
        def bucketDir = new File(tmpSlotDir, 'big-string/0')
        bucketDir.mkdirs()

        and: 'one valid big-string file'
        bigStringFiles.writeBigStringBytes(1234L, 0, 5678L, 'valid'.bytes)

        and: 'stray files that would previously throw NumberFormatException / ArrayIndexOutOfBoundsException'
        def strayNoUnderscore = new File(bucketDir, 'not_a_uuid_at_all')
        strayNoUnderscore.text = 'junk'
        def strayNonNumeric = new File(bucketDir, 'abc_def')
        strayNonNumeric.text = 'junk'
        def straySinglePart = new File(bucketDir, '12345')
        straySinglePart.text = 'junk'
        def straySubdir = new File(bucketDir, 'not_a_real_uuid')
        straySubdir.mkdirs()

        when: 'getBigStringFileIdList is called on a bucket with stray files'
        def list = bigStringFiles.getBigStringFileIdList(0)

        then: 'only the valid file is returned, no exception is thrown'
        list.size() == 1
        list[0].uuid() == 1234L
        list[0].bucketIndex() == 0
        list[0].keyHash() == 5678L

        when: 'getBigStringFileIdList is called on a non-existent bucket directory'
        def emptyList = bigStringFiles.getBigStringFileIdList(99999)

        then: 'returns empty list without throwing (listFiles returns null)'
        emptyList.isEmpty()

        cleanup:
        tmpSlotDir.deleteDir()
    }
}
