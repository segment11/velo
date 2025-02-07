package io.velo.rdb

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class RedisCrcTest extends Specification {
    def 'test all'() {
        final String libFileName = 'libredis_crc64.so'
        def f = new File('./' + libFileName)
        println f.absolutePath
        if (!f.exists()) {
            println 'lib not found'
            return
        }

        final String javaPackageDir = '/usr/java/packages/lib/'
        final File dir2 = new File(javaPackageDir)
        if (!dir2.exists()) {
            println 'create dir ' + javaPackageDir
            dir2.mkdirs()
            println 'create dir done'
        }

        def f2 = new File(dir2, libFileName)
        if (!f2.exists()) {
            println 'copy lib to ' + javaPackageDir
            FileUtils.copyFile(f, f2)
            println 'copy lib done'
        }

        given:
        RedisCrc.crc64Init()

        when:
        def r = RedisCrc.crc64(0, '123456789'.bytes, 9)
        println 'crc64: ' + r
        then:
        r == -1601353934260610614L

        when:
        def r2 = RedisCrc.crc64(0, new byte[0], 0)
        then:
        r2 == 0
    }
}
