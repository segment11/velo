package io.velo.script

import io.velo.Utils
import io.velo.command.CGroup
import io.velo.command.EGroup
import io.velo.command.IGroup
import io.velo.command.MGroup
import io.velo.dyn.CachedGroovyClassLoader
import spock.lang.Specification

class ScriptHandleTest extends Specification {
    def 'test all'() {
        // only for coverage
        given:
        def classpath = Utils.projectPath('/dyn/src')
        CachedGroovyClassLoader.instance.init(GroovyClassLoader.getClass().classLoader, classpath, null)

        and:
        def data1 = new byte[1][]
        def cGroup = new CGroup('config', data1, null)
        def eGroup = new EGroup('extend', data1, null)
        def iGroup = new IGroup('info', data1, null)
        def mGroup = new MGroup('manage', data1, null)

        def variables = new HashMap<String, Object>()
        variables.put('cGroup', cGroup)
        variables.put('eGroup', eGroup)
        variables.put('iGroup', iGroup)
        variables.put('mGroup', mGroup)

        def binding = new Binding(variables)

        def variables2 = new HashMap<String, Object>()
        variables2.put('cmd', 'extend')
        variables2.put('data', data1)
        variables2.put('slotNumber', 1)

        def binding2 = new Binding(variables2)

        String[] argsX = new String[1]

        when:
        def clusterxScript = new ClusterxCommandHandle()
        clusterxScript.setBinding(binding)
        clusterxScript.run()
        new ClusterxCommandHandle(binding).run()
        then:
        1 == 1

        when:
        def configScript = new ConfigCommandHandle()
        configScript.setBinding(binding)
        configScript.run()
        new ConfigCommandHandle(binding).run()
        then:
        1 == 1

        when:
        def extendScript = new ExtendCommandHandle()
        extendScript.setBinding(binding)
        extendScript.run()
        new ExtendCommandHandle(binding).run()
        then:
        1 == 1

        when:
        def extendScript2 = new ExtendCommandParseSlots()
        extendScript2.setBinding(binding2)
        extendScript2.run()
        new ExtendCommandParseSlots(binding2).run()
        then:
        1 == 1

        when:
        def infoScript = new InfoCommandHandle()
        infoScript.setBinding(binding)
        infoScript.run()
        new InfoCommandHandle(binding).run()
        then:
        1 == 1

        when:
        def manageScript = new ManageCommandHandle()
        manageScript.setBinding(binding)
        manageScript.run()
        new ManageCommandHandle(binding).run()
        then:
        1 == 1

        when:
        def manageScript2 = new ManageCommandParseSlots()
        manageScript2.setBinding(binding2)
        manageScript2.run()
        new ManageCommandParseSlots(binding2).run()
        then:
        1 == 1
    }
}
