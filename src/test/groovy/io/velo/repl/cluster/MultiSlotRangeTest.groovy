package io.velo.repl.cluster

import spock.lang.Specification

class MultiSlotRangeTest extends Specification {
    def 'test all'() {
        given:
        def m0 = new MultiSlotRange()
        def m1 = new MultiSlotRange()
        println m0
        println m0.clusterNodesSlotRangeList('xxx', 'localhost', 7379)

        expect:
        m0.slotCount() == 0
        m0 < m1
        m1 < m0
        !m0.contains(0)
        m0.toTreeSet().isEmpty()

        when:
        m0.addSingle(0, 4095)
        m0.addSingle(4096, 8191)
        println m0 > m1
        println m1 < m0
        m1.addSingle(8192, 16383)
        println m0
        println m1
        println m0.clusterNodesSlotRangeList('xxx', 'localhost', 7379)
        then:
        m0 < m1
        m1 > m0
        m0.contains(0)
        !m0.contains(8192)

        when:
        def mxx = MultiSlotRange.fromSelfString('')
        def m00 = MultiSlotRange.fromSelfString(m0.toString())
        then:
        mxx.slotCount() == 0
        m00.slotCount() == 8192
        m00.toTreeSet().size() == 8192

        when:
        boolean exception = false
        try {
            MultiSlotRange.fromSelfString('a-b')
        } catch (NumberFormatException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            MultiSlotRange.fromSelfString('1-0')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            MultiSlotRange.fromSelfString('0-1-2')
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        m0.removeOneSlot(0)
        m0.removeOneSlot(0)
        m1.addOneSlot(0)
        m1.addOneSlot(0)
        then:
        m0.slotCount() == 8191
        m1.slotCount() == 8193

        when:
        TreeSet<Integer> removeSet = []
        TreeSet<Integer> addSet = []
        m0.removeOrAddSet(removeSet, addSet)
        removeSet << 1 << 2 << 3
        m0.removeOrAddSet(removeSet, addSet)
        addSet << 1 << 2 << 3
        removeSet.clear()
        m1.removeOrAddSet(removeSet, addSet)
        then:
        m0.slotCount() == 8188
        m1.slotCount() == 8196
    }
}
