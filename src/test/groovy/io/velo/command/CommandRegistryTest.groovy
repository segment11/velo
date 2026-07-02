package io.velo.command

import io.velo.command.CommandEntry
import spock.lang.Specification

class CommandRegistryTest extends Specification {

    // Snapshot of the registry taken before each feature clears it, so the
    // production singleton (populated by group static blocks) is restored in
    // cleanup. Static initializers do not re-run after class loading, so a bare
    // clear() would permanently erase registrations in a shared-JVM test run.
    private Collection<CommandEntry> savedSnapshot

    def setup() {
        savedSnapshot = CommandRegistry.all()
        CommandRegistry.clear()
    }

    def cleanup() {
        CommandRegistry.clear()
        for (CommandEntry entry : savedSnapshot) {
            CommandRegistry.register(entry)
        }
    }

    def 'test register and get'() {
        given:
        def entry = new CommandEntry('get', 2,
                Set.of('readonly', 'fast'),
                1, 1, 1,
                Set.of('@read', '@string', '@fast'),
                'string', 'Get the value of a key.', '1.0.0', 'O(1)')

        when:
        CommandRegistry.register(entry)

        then:
        CommandRegistry.size() == 1
        CommandRegistry.get('get') == entry
        CommandRegistry.get('nope') == null
        CommandRegistry.get((String) null) == null
    }

    def 'test get is case insensitive'() {
        given:
        def entry = new CommandEntry('get', 2,
                Set.of('readonly'), 1, 1, 1, Set.of('@read'),
                'string', 'Get the value of a key.', '1.0.0', 'O(1)')

        when:
        CommandRegistry.register(entry)

        then:
        CommandRegistry.get('GET') == entry
        CommandRegistry.get('Get') == entry
    }

    def 'test duplicate name warns and skips keeping first'() {
        given:
        def first = new CommandEntry('set', 3,
                Set.of('write'), 1, 1, 1, Set.of('@write'),
                'string', 'first summary', '1.0.0', 'O(1)')
        def second = new CommandEntry('set', -3,
                Set.of('write'), 1, 1, 1, Set.of('@write'),
                'string', 'second summary', '2.0.0', null)

        when:
        CommandRegistry.register(first)
        CommandRegistry.register(second)

        then:
        CommandRegistry.size() == 1
        CommandRegistry.get('set').summary() == 'first summary'
        CommandRegistry.get('set').arity() == 3
    }

    def 'test all returns entries in insertion order'() {
        given:
        CommandRegistry.register(new CommandEntry('get', 2,
                Set.of(), 1, 1, 1, Set.of(), 'string', 'g', '1.0.0', null))
        CommandRegistry.register(new CommandEntry('set', 3,
                Set.of(), 1, 1, 1, Set.of(), 'string', 's', '1.0.0', null))
        CommandRegistry.register(new CommandEntry('del', 2,
                Set.of(), 1, 1, 1, Set.of(), 'generic', 'd', '1.0.0', null))

        when:
        def names = CommandRegistry.all().collect { it.name() }

        then:
        names == ['get', 'set', 'del']
    }

    def 'test all returns a snapshot immune to mutation'() {
        given:
        CommandRegistry.register(new CommandEntry('get', 2,
                Set.of(), 1, 1, 1, Set.of(), 'string', 'g', '1.0.0', null))

        when:
        def snapshot = CommandRegistry.all()
        CommandRegistry.clear()

        then:
        snapshot.size() == 1
        snapshot[0].name() == 'get'
    }
}
