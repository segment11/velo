package io.velo.repl.cluster

import spock.lang.Specification

class SlotRangeTest extends Specification {
    def 'test all'() {
        given:
        def slotRange = new SlotRange(1, 2)
        println slotRange
        println new SlotRange(1, 1)

        slotRange.begin = 1
        slotRange.end = 2

        expect:
        slotRange.begin == 1
        slotRange.end == 2
        slotRange.contains(1)
        slotRange.contains(2)
        !slotRange.contains(0)
        !slotRange.contains(3)
        slotRange.slotCount() == 2
        slotRange.toString() == '1-2'
        slotRange > new SlotRange(0, 1)
        slotRange < new SlotRange(2, 3)
    }
}
