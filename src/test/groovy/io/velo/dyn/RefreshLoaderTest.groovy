package io.velo.dyn


import spock.lang.Specification

class RefreshLoaderTest extends Specification {
    def 'concurrent getScriptText does not throw ConcurrentModificationException'() {
        when:
        def errors = Collections.synchronizedList(new ArrayList<Throwable>())
        def done = new java.util.concurrent.CountDownLatch(4)
        def scriptPath = '/dyn/src/io/velo/script/ManageCommandParseSlots.groovy'

        4.times { idx ->
            Thread.start {
                try {
                    200.times {
                        def text = RefreshLoader.getScriptText(scriptPath)
                        assert text != null && text.length() > 0
                    }
                } catch (Throwable t) {
                    errors << t
                } finally {
                    done.countDown()
                }
            }
        }

        done.await(10, java.util.concurrent.TimeUnit.SECONDS)

        then:
        errors.isEmpty()
    }

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
