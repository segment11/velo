package io.velo


import spock.lang.Specification

class AfterAuthFlagHolderTest extends Specification {
    def 'test all'() {
        given:
        def remoteAddress = new InetSocketAddress('localhost', 46379)
        AfterAuthFlagHolder.add(remoteAddress)

        expect:
        AfterAuthFlagHolder.contains(remoteAddress)

        when:
        AfterAuthFlagHolder.remove(remoteAddress)
        then:
        !AfterAuthFlagHolder.contains(remoteAddress)
    }
}
