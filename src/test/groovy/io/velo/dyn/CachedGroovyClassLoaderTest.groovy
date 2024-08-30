package io.velo.dyn

import io.velo.Utils
import spock.lang.Specification

class CachedGroovyClassLoaderTest extends Specification {
    def 'parse class'() {
        given:
        def loader = CachedGroovyClassLoader.instance
        def classpath = Utils.projectPath('/dyn/src')
        loader.init(GroovyClassLoader.getClass().classLoader, classpath, null)

        def clz = loader.gcl.parseClass(new File('dyn/src/Test.groovy'))
        def clz2 = loader.gcl.parseClass(new File('dyn/src/Test.groovy'))
        println clz
        println clz2
        def obj = clz.newInstance()

        expect:
        clz == clz2
        obj.hi() == 'hi kerry'
        loader.eval('"hi"') == 'hi'

        when:
        // compile static
        def sss = '''
def name = super.binding.getProperty('name')
"hi ${name}"
'''
        then:
        loader.eval(sss, [name: 'kerry']) == 'hi kerry'
    }
}
