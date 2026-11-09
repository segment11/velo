package io.velo.repl.incremental

import spock.lang.Specification

class XDynConfigTest extends Specification {
    def 'test stub methods throw UnsupportedOperationException'() {
        given:
        def xDynConfig = new XDynConfig()

        when:
        xDynConfig.encodedLength()
        then:
        thrown(UnsupportedOperationException)

        when:
        xDynConfig.encodeWithType()
        then:
        thrown(UnsupportedOperationException)

        when:
        XDynConfig.decodeFrom(null)
        then:
        thrown(UnsupportedOperationException)

        when:
        xDynConfig.apply((short) 0, null)
        then:
        thrown(UnsupportedOperationException)
    }
}
