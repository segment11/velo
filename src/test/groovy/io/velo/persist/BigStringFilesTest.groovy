package io.velo.persist

import io.velo.CompressedValue
import io.velo.ConfForGlobal
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
        println new BigStringFiles.IdWithKey(1234L, '1234')

        def tmpSlotDir1 = new File('/tmp/tmp-slot-dir')
        def tmpSlotDir2 = new File('/tmp/tmp-slot-dir2')
        if (tmpSlotDir1.exists()) {
            tmpSlotDir1.deleteDir()
        }
        if (!tmpSlotDir2.exists()) {
            tmpSlotDir2.mkdirs()
        }

        ConfForGlobal.pureMemory = false
        def bigStringFiles1 = new BigStringFiles(slot, tmpSlotDir1)
        def bigStringFiles11 = new BigStringFiles(slot, tmpSlotDir1)
        def bigStringFiles2 = new BigStringFiles(slot, tmpSlotDir2)
        println bigStringFiles1.estimate(new StringBuilder())
        bigStringFiles1.collect()

        when:
        def isWriteOk = bigStringFiles1.writeBigStringBytes(1L, 'a', 0, bigString.bytes)
        then:
        isWriteOk
        bigStringFiles1.getBigStringBytes(1L, 0) == bigString.bytes
        bigStringFiles1.getBigStringBytes(1L, 0, true) == bigString.bytes
        bigStringFiles1.getBigStringBytes(1L, 0, true) == bigString.bytes
        bigStringFiles1.getBigStringFileUuidList(0).size() == 1
        bigStringFiles11.getBigStringFileUuidList(0).size() == 1
        bigStringFiles2.getBigStringBytes(1L, 0) == null

        when:
        def bigStringFiles111 = new BigStringFiles(slot, tmpSlotDir1)
        then:
        bigStringFiles111.bucketIndexesWhenFirstServerStart.size() == 1

        when:
        bigStringFiles1.deleteBigStringFileIfExist(1L, 0)
        bigStringFiles2.deleteBigStringFileIfExist(1L, 0)
        bigStringFiles1.deleteAllBigStringFiles()
        bigStringFiles2.deleteAllBigStringFiles()
        then:
        bigStringFiles1.getBigStringFileUuidList(0).size() == 0
        bigStringFiles2.getBigStringFileUuidList(0).size() == 0
    }

    def 'test pure memory mode'() {
        given:
        def bigString = 'a' * 10000

        ConfForGlobal.pureMemory = true
        def bigStringFiles = new BigStringFiles(slot, null)
        println bigStringFiles.estimate(new StringBuilder())

        when:
        def isWriteOk = bigStringFiles.writeBigStringBytes(1L, 'a', 0, bigString.bytes)
        then:
        isWriteOk
        bigStringFiles.getBigStringBytes(1L, 0) == bigString.bytes
        bigStringFiles.getBigStringFileUuidList(0).size() == 1

        when:
        bigStringFiles.deleteBigStringFileIfExist(1L, 0)
        then:
        bigStringFiles.getBigStringFileUuidList(0).size() == 0

        when:
        bigStringFiles.writeBigStringBytes(1L, 'a', 0, bigString.bytes)
        // skip
        bigStringFiles.handleWhenCvExpiredOrDeleted('a', null, null)
        def cv = new CompressedValue()
        // skip as cv type is not big string
        bigStringFiles.handleWhenCvExpiredOrDeleted('a', cv, null)
        cv.dictSeqOrSpType = CompressedValue.SP_TYPE_BIG_STRING
        cv.compressedData = new byte[8]
        // uuid not match
        bigStringFiles.handleWhenCvExpiredOrDeleted('a', cv, null)
        then:
        1 == 1

        when:
        def bos = new ByteArrayOutputStream()
        def os = new DataOutputStream(bos)
        bigStringFiles.writeToSavedFileWhenPureMemory(os)
        def bis = new ByteArrayInputStream(bos.toByteArray())
        def is = new DataInputStream(bis)
        bigStringFiles.loadFromLastSavedFileWhenPureMemory(is)
        then:
        bigStringFiles.getBigStringFileUuidList(0).size() == 1

        cleanup:
        bigStringFiles.deleteAllBigStringFiles()
        ConfForGlobal.pureMemory = false
    }

    def 'test write io exception'() {
        given:
        def noPermitDir = new File('/usr/tmp-slot-dir')

        ConfForGlobal.pureMemory = false

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
        def isWriteOk = bigStringFiles.writeBigStringBytes(1L, 'a', 0, bigString.bytes)

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
        bigStringFiles.getBigStringBytes(1L, 0) == null

        cleanup:
        // delete dir
        Files.setAttribute(bigStringDir.toPath(),
                'posix:permissions', PosixFilePermissions.fromString('rwxrwxrwx'))
        bigStringDir.deleteDir()
    }
}
