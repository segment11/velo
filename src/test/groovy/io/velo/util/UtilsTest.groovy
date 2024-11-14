package io.velo.util

import spock.lang.Specification

class UtilsTest extends Specification {
    def 'test generate random chars'() {
        given:
        def randomChars = Utils.generateRandomChars(10)
        println randomChars
        expect:
        randomChars.size() == 10
    }
}
