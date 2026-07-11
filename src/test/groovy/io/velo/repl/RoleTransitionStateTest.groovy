package io.velo.repl

import spock.lang.Specification

class RoleTransitionStateTest extends Specification {
    def 'test durable role transition state round trip and clear'() {
        given:
        def dir = File.createTempDir('velo-role-transition-', '')
        def store = new RoleTransitionState.Store(dir)
        def target = new RoleTransitionState.BinlogTarget(3, 262144L)
        def state = RoleTransitionState.begin(7L, RoleTransitionState.TargetRole.MASTER, '127.0.0.1', 7379,
                [(Short.valueOf((short) 0)): target])
                .withPhase(RoleTransitionState.Phase.PREPARING)
                .withPreparedSlot((short) 0)

        when:
        store.save(state)
        def loaded = store.load()

        then:
        loaded == state
        loaded.binlogTargets()[(short) 0] == target
        loaded.preparedSlots() == [(short) 0] as Set

        when:
        store.clear()

        then:
        store.load() == null

        cleanup:
        dir.deleteDir()
    }

    def 'test role transition updates preserve immutable target and epoch'() {
        given:
        def target = new RoleTransitionState.BinlogTarget(2, 0L)
        def state = RoleTransitionState.begin(11L, RoleTransitionState.TargetRole.SLAVE, 'master', 7380,
                [(Short.valueOf((short) 1)): target])

        when:
        def updated = state.withPreparedSlot((short) 1).withCommittedSlot((short) 1)
                .withPhase(RoleTransitionState.Phase.COMMITTING)

        then:
        updated.epoch() == 11L
        updated.binlogTargets()[(short) 1] == target
        updated.preparedSlots() == [(short) 1] as Set
        updated.committedSlots() == [(short) 1] as Set
        state.preparedSlots().empty
        state.committedSlots().empty
    }

    def 'test corrupt role transition state fails closed'() {
        given:
        def dir = File.createTempDir('velo-role-transition-', '')
        new File(dir, RoleTransitionState.Store.FILE_NAME).text = '{broken'

        when:
        new RoleTransitionState.Store(dir).load()

        then:
        thrown(IOException)

        cleanup:
        dir.deleteDir()
    }
}
