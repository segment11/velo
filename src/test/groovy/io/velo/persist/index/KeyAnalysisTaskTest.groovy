package io.velo.persist.index

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.ConfForGlobal
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import org.rocksdb.WriteBatch
import org.rocksdb.WriteOptions
import spock.lang.Specification

import java.time.Duration

class KeyAnalysisTaskTest extends Specification {
    def 'test all'() {
        given:
        def config = Config.create()
                .with('notBusyBeginTime', '00:00:00.0')
                .with('notBusyEndTime', '00:00:00.0')
        def persistConfig = Config.create()
                .with('keyAnalysis', config)

        and:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        and:
        def keyDir = new File(Consts.persistDir, 'keys-for-analysis')
        if (!keyDir.exists()) {
            keyDir.mkdirs()
        }

        ConfForGlobal.keyAnalysisNumberPercent = 100
        ConfForGlobal.estimateKeyNumber = 1_000_000
        def keyAnalysisHandler = new KeyAnalysisHandler(keyDir, eventloop, persistConfig)
        def keyAnalysisTask = keyAnalysisHandler.innerTask as KeyAnalysisTask
        keyAnalysisTask.addTopKPrefixCount('test:', 0)

        and:
        def localPersist = LocalPersist.instance
        localPersist.addOneSlot((short) 0, eventloop)
        localPersist.debugMode()

        when:
        Thread.sleep(100)
        100.times {
            keyAnalysisTask.run(it)
        }
        then:
        // now if not in time range
        !keyAnalysisTask.isNotBusyAfterRun

        when:
        def config2 = Config.create()
                .with('notBusyBeginTime', '00:00:00.0')
                .with('notBusyEndTime', '23:59:59.0')
        def persistConfig2 = Config.create()
                .with('keyAnalysis', config2)

        def keyAnalysisTask2 = new KeyAnalysisTask(keyAnalysisHandler, null, keyAnalysisHandler.db, persistConfig2)
        keyAnalysisHandler.addCount = 10000
        keyAnalysisTask2.run(1)
        then:
        // now is in time range, but add count increased is > 1000
        !keyAnalysisTask2.isNotBusyAfterRun

        when:
        keyAnalysisTask2.run(2)
        then:
        keyAnalysisTask2.isNotBusyAfterRun

        when:
        def random = new Random()
        def wb = new WriteBatch()
        19000.times {
            def xx = random.nextInt(10).toString() + it.toString().padLeft(12, '0')
            def key = 'key:' + xx
            def value = 'value:' + xx
            wb.put(key.bytes, value.bytes)
        }
        keyAnalysisHandler.db.write(new WriteOptions(), wb)
        println 'done write batch, count 19000'
        keyAnalysisTask2.doMyTaskSkipTimes = 1
        // skip one time
        keyAnalysisTask2.run(3)
        keyAnalysisTask2.run(3)
        then:
        keyAnalysisTask2.topKPrefixCounts.size() > 0

        when:
        keyAnalysisTask2.run(4)
        then:
        keyAnalysisTask2.topKPrefixCounts.size() > 0

        cleanup:
        keyAnalysisHandler.flushdb()
        Thread.sleep(1000)
        eventloop.breakEventloop()
        localPersist.cleanUp()
        keyDir.deleteDir()
    }

    def 'test all in pure memory mode'() {
        given:
        ConfForGlobal.pureMemory = true
        def config = Config.create()
                .with('notBusyBeginTime', '00:00:00.0')
                .with('notBusyEndTime', '00:00:00.0')
        def persistConfig = Config.create()
                .with('keyAnalysis', config)

        and:
        def eventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        eventloop.keepAlive(true)
        Thread.start {
            eventloop.run()
        }

        and:
        def keyDir = new File(Consts.persistDir, 'keys-for-analysis')
        if (!keyDir.exists()) {
            keyDir.mkdirs()
        }

        ConfForGlobal.keyAnalysisNumberPercent = 100
        ConfForGlobal.estimateKeyNumber = 1_000_000
        def keyAnalysisHandler = new KeyAnalysisHandler(keyDir, eventloop, persistConfig)
        def keyAnalysisTask = keyAnalysisHandler.innerTask as KeyAnalysisTask
        keyAnalysisTask.addTopKPrefixCount('test:', 0)

        and:
        def localPersist = LocalPersist.instance
        localPersist.addOneSlot((short) 0, eventloop)
        localPersist.debugMode()

        when:
        Thread.sleep(100)
        100.times {
            keyAnalysisTask.run(it)
        }
        then:
        // now if not in time range
        !keyAnalysisTask.isNotBusyAfterRun

        when:
        def config2 = Config.create()
                .with('notBusyBeginTime', '00:00:00.0')
                .with('notBusyEndTime', '23:59:59.0')
        def persistConfig2 = Config.create()
                .with('keyAnalysis', config2)

        def keyAnalysisTask2 = new KeyAnalysisTask(keyAnalysisHandler, keyAnalysisHandler.allKeysInMemory, keyAnalysisHandler.db, persistConfig2)
        keyAnalysisHandler.addCount = 10000
        keyAnalysisTask2.run(1)
        then:
        // now is in time range, but add count increased is > 1000
        !keyAnalysisTask2.isNotBusyAfterRun

        when:
        keyAnalysisTask2.run(2)
        then:
        keyAnalysisTask2.isNotBusyAfterRun

        when:
        def random = new Random()
        19000.times {
            def xx = random.nextInt(10).toString() + it.toString().padLeft(12, '0')
            def key = 'key:' + xx
            def value = 'value:' + xx
            keyAnalysisHandler.allKeysInMemory.put(key, value.bytes)
        }
        println 'done write batch, count 19000'
        keyAnalysisTask2.doMyTaskSkipTimes = 1
        // skip one time
        keyAnalysisTask2.run(3)
        keyAnalysisTask2.run(3)
        then:
        keyAnalysisTask2.topKPrefixCounts.size() > 0

        when:
        keyAnalysisTask2.run(4)
        then:
        keyAnalysisTask2.topKPrefixCounts.size() > 0

        cleanup:
        keyAnalysisHandler.flushdb()
        Thread.sleep(1000)
        eventloop.breakEventloop()
        localPersist.cleanUp()
        keyDir.deleteDir()
        ConfForGlobal.pureMemory = false
    }
}
