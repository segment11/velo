package io.velo.monitor

import spock.lang.Specification

class RuntimeCpuCollectorTest extends Specification {
    static volatile boolean isStop = false

    def 'test collect'() {
        given:
        int seconds = 5 * 10
        int intervalSeconds = 5

        when:
        Thread.start {
            for (i in 0..<(seconds / intervalSeconds).intValue()) {
                Thread.sleep(1000 * intervalSeconds)
                def p = RuntimeCpuCollector.collect()
                println p.getKernelTime() + ',' + p.getUserTime()
            }
            println 'done cpu collection'
            isStop = true
        }

        Thread.start {
            long i = 0
            while (!isStop) {
                i++
            }
            println 'done busy counter'
        }

        Thread.start {
            int i = 0
            while (!isStop) {
                Thread.sleep(1)
                i++
            }
            println 'done counter'
        }.join()

        then:
        1 == 1

        cleanup:
        RuntimeCpuCollector.close()
    }
}
