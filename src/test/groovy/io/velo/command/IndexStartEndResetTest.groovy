package io.velo.command

import spock.lang.Specification

class IndexStartEndResetTest extends Specification {
    def 'test all'() {
        given:
        int valueLength = 6

        when:
        def after = IndexStartEndReset.reset(0, 0, valueLength)
        then:
        after.valid()
        after.start() == 0
        after.end() == 0

        when:
        after = IndexStartEndReset.reset(0, 6, valueLength)
        then:
        after.valid()
        after.start() == 0
        after.end() == 5

        when:
        after = IndexStartEndReset.reset(3, 2, valueLength)
        then:
        !after.valid()

        when:
        after = IndexStartEndReset.reset(6, 2, valueLength)
        then:
        !after.valid()

        when:
        after = IndexStartEndReset.reset(0, -1, valueLength)
        then:
        after.valid()
        after.start() == 0
        after.end() == 5

        when:
        after = IndexStartEndReset.reset(0, -7, valueLength)
        then:
        !after.valid()

        when:
        after = IndexStartEndReset.reset(-2, -1, valueLength)
        then:
        after.valid()
        after.start() == 4
        after.end() == 5

        when:
        after = IndexStartEndReset.reset(-7, 1, valueLength)
        then:
        after.valid()
        after.start() == 0
        after.end() == 1
    }
}
