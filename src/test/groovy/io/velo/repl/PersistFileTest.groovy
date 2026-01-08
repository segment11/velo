package io.velo.repl

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class PersistFileTest extends Specification {
    def 'test all'() {
        given:
        def file = new File('/tmp/test_file.dat')
        if (!file.exists()) {
            FileUtils.touch(file)
        }

        and:
        def persistFile = new PersistFile(file, 0)

        expect:
        persistFile.fileIndex() == 0
        persistFile.length() == 0
        persistFile.name == file.name

        cleanup:
        persistFile.delete()
    }
}
