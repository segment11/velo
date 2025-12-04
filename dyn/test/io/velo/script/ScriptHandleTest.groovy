package io.velo.script

import io.velo.MultiWorkerServer
import io.velo.SocketInspector
import io.velo.Utils
import io.velo.command.*
import io.velo.dyn.CachedGroovyClassLoader
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import spock.lang.Specification

class ScriptHandleTest extends Specification {
    def 'test all'() {
        // only for coverage
        given:
        def classpath = Utils.projectPath('/dyn/src')
        CachedGroovyClassLoader.instance.init(GroovyClassLoader.getClass().classLoader, classpath, null)

        and:
        final short slot = 0
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        and:
        def data1 = new byte[1][]
        def cGroup = new CGroup('config', data1, null)
        def cGroup11 = new CGroup('command', data1, null)
        def eGroup = new EGroup('extend', data1, null)
        def iGroup = new IGroup('info', data1, null)
        def mGroup = new MGroup('manage', data1, null)
        def sGroup = new SGroup('sentinel', data1, null)

        def variables = new HashMap<String, Object>()
        variables.put('cGroup', cGroup)
        variables.put('eGroup', eGroup)
        variables.put('iGroup', iGroup)
        variables.put('mGroup', mGroup)
        variables.put('sGroup', sGroup)

        def binding = new Binding(variables)

        def variables2 = new HashMap<String, Object>()
        variables2.put('cmd', 'extend')
        variables2.put('data', data1)
        variables2.put('slotNumber', 1)

        def variables3 = new HashMap<String, Object>()
        variables3.put('cGroup', cGroup11)

        def binding2 = new Binding(variables2)
        def binding3 = new Binding(variables3)

        String[] argsX = new String[1]

        when:
        def clusterxScript = new ClusterxCommandHandle()
        clusterxScript.setBinding(binding)
        clusterxScript.run()
        then:
        1 == 1

        when:
        def commandScript = new CommandCommandHandle()
        commandScript.setBinding(binding3)
        commandScript.run()
        then:
        1 == 1

        when:
        def configScript = new ConfigCommandHandle()
        configScript.setBinding(binding)
        configScript.run()
        then:
        1 == 1

        when:
        def extendScript = new ExtendCommandHandle()
        extendScript.setBinding(binding)
        extendScript.run()
        then:
        1 == 1

        when:
        def extendScript2 = new ExtendCommandParseSlots()
        extendScript2.setBinding(binding2)
        extendScript2.run()
        then:
        1 == 1

        when:
        def inspector = new SocketInspector()
        MultiWorkerServer.STATIC_GLOBAL_V.socketInspector = inspector
        def infoScript = new InfoCommandHandle()
        infoScript.setBinding(binding)
        infoScript.run()
        then:
        1 == 1

        when:
        def manageScript = new ManageCommandHandle()
        manageScript.setBinding(binding)
        manageScript.run()
        then:
        1 == 1

        when:
        def manageScript2 = new ManageCommandParseSlots()
        manageScript2.setBinding(binding2)
        manageScript2.run()
        then:
        1 == 1

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
