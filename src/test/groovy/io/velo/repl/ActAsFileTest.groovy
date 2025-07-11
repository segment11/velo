package io.velo.repl

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class ActAsFileTest extends Specification {
    def 'test all'() {
        given:
        def file = new File('/tmp/test_file.dat')
        if (!file.exists()) {
            FileUtils.touch(file)
        }

        and:
        def actAsFile = new ActAsFile.PersistFile(file, 0)

        expect:
        actAsFile.fileIndex() == 0
        actAsFile.length() == 0
        actAsFile.name == file.name

        cleanup:
        actAsFile.delete()
    }
}
