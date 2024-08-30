package io.velo.repl.support


import spock.lang.Specification

class ExtendProtocolCommandTest extends Specification {
    // only for coverage
    def 'test all'() {
        given:
        def command = new ExtendProtocolCommand('test')

        expect:
        command.raw == 'test'.bytes
    }
}
