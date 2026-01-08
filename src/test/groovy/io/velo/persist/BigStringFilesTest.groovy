package io.velo.persist

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
        bigStringFiles1.getBigStringBytes(1L, 0, 1L) == bigString.bytes
        bigStringFiles1.getBigStringBytes(1L, 1, 1L) == null
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
}
