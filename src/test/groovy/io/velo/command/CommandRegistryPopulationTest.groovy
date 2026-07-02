package io.velo.command

import spock.lang.Specification

class CommandRegistryPopulationTest extends Specification {

    def 'test all command groups are registered'() {
        when:
        // reference every group class to trigger static initialization
        new AGroup(null, null, null)
        new BGroup(null, null, null)
        new CGroup(null, null, null)
        new DGroup(null, null, null)
        new EGroup(null, null, null)
        new FGroup(null, null, null)
        new GGroup(null, null, null)
        new HGroup(null, null, null)
        new IGroup(null, null, null)
        new JGroup(null, null, null)
        new KGroup(null, null, null)
        new LGroup(null, null, null)
        new MGroup(null, null, null)
        new NGroup(null, null, null)
        new OGroup(null, null, null)
        new PGroup(null, null, null)
        new QGroup(null, null, null)
        new RGroup(null, null, null)
        new SGroup(null, null, null)
        new TGroup(null, null, null)
        new UGroup(null, null, null)
        new VGroup(null, null, null)
        new WGroup(null, null, null)
        new XGroup(null, null, null)
        new YGroup(null, null, null)
        new ZGroup(null, null, null)
        def size = CommandRegistry.size()

        then:
        size == 212

        and: 'a representative known command is present'
        CommandRegistry.get('get') != null
        CommandRegistry.get('zadd') != null
        CommandRegistry.get('bf.add') != null
        CommandRegistry.get('clusterx') != null

        and: 'all names are unique and lowercase-keyed'
        CommandRegistry.all().collect { it.name() } as Set ==
                CommandRegistry.all().collect { it.name().toLowerCase() } as Set
    }
}
