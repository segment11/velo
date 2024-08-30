package io.velo.dyn


import spock.lang.Specification

class RefreshLoaderTest extends Specification {
    def 'refresh'() {
        given:
        def rootPath = new File('.').absolutePath.replaceAll("\\\\", '/').
                replace(this.class.name.replaceAll('.', '/'), '')
        println rootPath

        def loader = CachedGroovyClassLoader.instance
        loader.init(Thread.currentThread().contextClassLoader, rootPath + '/dyn/src', null)

        when:
        def refreshLoader = RefreshLoader.create(loader.gcl)
                .addDir(rootPath + '/dyn/src/io/velo')
                .addDir(rootPath + '/dyn/not_exist_dir')
                .refreshFileCallback { File file ->
                    println 'eval groovy file callback, file: ' + file.name
                }
        refreshLoader.refresh()
        refreshLoader.refresh()
        then:
        1 == 1
    }
}
