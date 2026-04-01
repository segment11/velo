package io.velo.type

import spock.lang.Specification

class CharsetCompatibilityTest extends Specification {
    def 'type containers keep utf8 members across non utf8 default charset process'() {
        given:
        def javaBin = new File(System.getProperty('java.home'), 'bin/java').absolutePath
        def classpath = System.getProperty('java.class.path')
        def process = new ProcessBuilder(
                javaBin,
                '-Dfile.encoding=ISO-8859-1',
                '-cp',
                classpath,
                'io.velo.type.CharsetCompatibilityMain'
        ).redirectErrorStream(true).start()

        when:
        def output = process.inputStream.text
        def exitCode = process.waitFor()

        then:
        assert exitCode == 0 : output
        output.trim().isEmpty()
    }
}
