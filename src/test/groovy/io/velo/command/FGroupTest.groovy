package io.velo.command

import io.activej.config.Config
import io.activej.eventloop.Eventloop
import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.index.IndexHandlerPool
import io.velo.persist.index.KeyAnalysisHandler
import io.velo.repl.Binlog
import io.velo.reply.*
import spock.lang.Specification

import java.time.Duration
import java.util.concurrent.CompletableFuture

class FGroupTest extends Specification {
    def _FGroup = new FGroup(null, null, null)

    def 'test parse slot'() {
        given:
        int slotNumber = 128
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        expect:
        _FGroup.parseSlots('failover', data2, slotNumber).size() == 0
    }

    def 'test handle'() {
        given:
        def data2 = new byte[2][]
        data2[1] = 'a'.bytes

        when: 'unknown flushdb token is a syntax error'
        def fGroup = new FGroup('flushdb', data2, null)
        fGroup.from(BaseCommand.mockAGroup())
        def reply = fGroup.handle()
        then:
        reply == ErrorReply.SYNTAX

        when: 'failover abort is answered'
        def abortData = new byte[2][]
        abortData[1] = 'abort'.bytes
        def fGroup2 = new FGroup('failover', abortData, null)
        fGroup2.from(BaseCommand.mockAGroup())
        reply = fGroup2.handle()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == 'No failover in progress.'

        when: 'unknown command falls through to nil'
        def fGroup3 = new FGroup('zzz', data2, null)
        fGroup3.from(BaseCommand.mockAGroup())
        reply = fGroup3.handle()
        then:
        reply == NilReply.INSTANCE
    }

    final short slot = 0
    final short slot1 = 1

    private static Reply resolveReply(Reply reply) {
        if (reply instanceof AsyncReply) {
            return ((AsyncReply) reply).settablePromise.getResult()
        }
        return reply
    }

    def 'test failover'() {
        given:
        def data1 = new byte[1][]

        def fGroup = new FGroup('failover', data1, null)
        fGroup.from(BaseCommand.mockAGroup())

        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 2)
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId(slot1, Thread.currentThread().threadId())
        def oneSlot0 = localPersist.oneSlot(slot)
        def oneSlot1 = localPersist.oneSlot(slot1)
        // dyn config persists between runs, start from a known writable state
        oneSlot0.setReadonly(false)
        oneSlot1.setReadonly(false)

        when: 'no slave on the first slot'
        def reply = fGroup.failover()
        then:
        resolveReply(reply) instanceof ErrorReply
        (resolveReply(reply) as ErrorReply).message == 'no slave'

        when: '76.1: FAILOVER ABORT is answered, no failover state exists to abort'
        def abortData = new byte[2][]
        abortData[1] = 'abort'.bytes
        def fGroupAbort = new FGroup('failover', abortData, null)
        fGroupAbort.from(BaseCommand.mockAGroup())
        reply = fGroupAbort.handle()
        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == 'No failover in progress.'

        when: '76.1: an unknown failover token is a syntax error'
        def unknownData = new byte[2][]
        unknownData[1] = 'nonsense'.bytes
        def fGroupUnknown = new FGroup('failover', unknownData, null)
        fGroupUnknown.from(BaseCommand.mockAGroup())
        reply = fGroupUnknown.handle()
        then:
        reply == ErrorReply.SYNTAX

        when: '76.2: FAILOVER TO targeting is not supported'
        def fGroupArgs = new FGroup(null, null, null)
        fGroupArgs.from(BaseCommand.mockAGroup())
        reply = fGroupArgs.execute('failover to localhost 6380')
        then:
        reply == ErrorReply.NOT_SUPPORT

        when: '76.2: FAILOVER TIMEOUT is not supported'
        reply = fGroupArgs.execute('failover timeout 1000')
        then:
        reply == ErrorReply.NOT_SUPPORT

        when: '76.2: FAILOVER ABORT combined with other options is rejected'
        reply = fGroupArgs.execute('failover abort timeout 1000')
        then:
        reply == ErrorReply.NOT_SUPPORT

        when: '76.2: FAILOVER FORCE is not supported'
        reply = fGroupArgs.execute('failover force')
        then:
        reply == ErrorReply.NOT_SUPPORT

        when: '76.2: unknown failover options are a syntax error'
        reply = fGroupArgs.execute('failover nonsense x')
        then:
        reply == ErrorReply.SYNTAX

        when: '76.4: first slot caught up but second slot lagging must abort the failover'
        def rp1 = oneSlot0.createIfNotExistReplPairAsMaster(11L, 'localhost', 6380)
        rp1.slaveLastCatchUpBinlogFileIndexAndOffset = oneSlot0.getBinlog().currentFileIndexAndOffset()
        def rp2 = oneSlot1.createIfNotExistReplPairAsMaster(12L, 'localhost', 6381)
        // rp2 catch up left null, lagging
        reply = fGroup.failover()
        then:
        resolveReply(reply) instanceof ErrorReply
        (resolveReply(reply) as ErrorReply).message.contains('slave not catch up')
        // fenced slots are rolled back when the failover aborts
        !oneSlot0.isReadonly()
        !oneSlot1.isReadonly()

        when: 'pair with null catch up'
        rp1.slaveLastCatchUpBinlogFileIndexAndOffset = null
        reply = fGroup.failover()
        then:
        resolveReply(reply) instanceof ErrorReply
        (resolveReply(reply) as ErrorReply).message == 'slave not catch up=localhost:6380'
        !oneSlot0.isReadonly()
        !oneSlot1.isReadonly()

        when: 'pair with offset mismatch'
        rp1.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(0, 1L)
        reply = fGroup.failover()
        then:
        resolveReply(reply) instanceof ErrorReply
        (resolveReply(reply) as ErrorReply).message.contains('slave not catch up=localhost:6380, current=')
        !oneSlot0.isReadonly()
        !oneSlot1.isReadonly()

        when: 'pair with file index mismatch'
        rp1.slaveLastCatchUpBinlogFileIndexAndOffset = new Binlog.FileIndexAndOffset(1, 0L)
        reply = fGroup.failover()
        then:
        resolveReply(reply) instanceof ErrorReply
        (resolveReply(reply) as ErrorReply).message.contains('slave not catch up=localhost:6380, current=FileIndexAndOffset{fileIndex=0, offset=0}, slave=FileIndexAndOffset{fileIndex=1, offset=0}')
        !oneSlot0.isReadonly()
        !oneSlot1.isReadonly()

        when: 'a slot already readonly before failover stays readonly when the failover aborts'
        rp1.slaveLastCatchUpBinlogFileIndexAndOffset = null
        oneSlot1.setReadonly(true)
        reply = fGroup.failover()
        then:
        resolveReply(reply) instanceof ErrorReply
        // rollback must not un-fence a slot this command did not fence
        oneSlot1.isReadonly()
        !oneSlot0.isReadonly()

        when: 'all slots caught up: failover succeeds and demotes every slot'
        oneSlot1.setReadonly(false)
        rp1.slaveLastCatchUpBinlogFileIndexAndOffset = oneSlot0.getBinlog().currentFileIndexAndOffset()
        rp2.slaveLastCatchUpBinlogFileIndexAndOffset = oneSlot1.getBinlog().currentFileIndexAndOffset()
        reply = fGroup.failover()
        then:
        resolveReply(reply) == OKReply.INSTANCE
        oneSlot0.isReadonly()
        oneSlot1.isReadonly()

        cleanup:
        oneSlot0.setReadonly(false)
        oneSlot1.setReadonly(false)
        localPersist.cleanUp()
    }

    def 'test flushdb'() {
        given:
        def data1 = new byte[1][]

        def fGroup = new FGroup('flushdb', data1, null)
        fGroup.from(BaseCommand.mockAGroup())

        def localPersist = LocalPersist.instance
        LocalPersistTest.prepareLocalPersist()
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when: '76.1: FLUSHDB ASYNC must actually flush'
        def sGroup = new SGroup(null, null, null)
        sGroup.from(BaseCommand.mockAGroup())
        sGroup.execute('set fgroupk value')
        (sGroup.execute('strlen fgroupk') as IntegerReply).integer == 6
        reply = fGroup.execute('flushdb async')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result -> result == OKReply.INSTANCE }.result
        sGroup.execute('strlen fgroupk') == IntegerReply.REPLY_0

        when: '76.1: FLUSHDB SYNC also flushes'
        sGroup.execute('set fgroupk2 value2')
        (sGroup.execute('strlen fgroupk2') as IntegerReply).integer == 7
        reply = fGroup.execute('flushdb sync')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result -> result == OKReply.INSTANCE }.result
        sGroup.execute('strlen fgroupk2') == IntegerReply.REPLY_0

        when: '76.1: unknown token is a syntax error and keeps the data'
        sGroup.execute('set fgroupk3 value3')
        reply = fGroup.execute('flushdb nonsense')
        then:
        reply == ErrorReply.SYNTAX
        (sGroup.execute('strlen fgroupk3') as IntegerReply).integer == 6

        when: '76.2: extra tokens are a syntax error and keep the data'
        sGroup.execute('set fgroupk4 value4')
        reply = fGroup.execute('flushdb async extra')
        then:
        reply == ErrorReply.SYNTAX
        (sGroup.execute('strlen fgroupk4') as IntegerReply).integer == 6

        when: '76.3: readonly slot (e.g. replica) must refuse the flush'
        oneSlot.setReadonly(true)
        def reply = fGroup.execute('flushdb')
        then:
        reply == ErrorReply.READONLY
        (reply as ErrorReply).message == "READONLY You can't write against a read only replica."

        when: '76.3: node-level role transition write fence must refuse the flush'
        oneSlot.setReadonly(false)
        localPersist.setRoleTransitionWriteFenced(true)
        reply = fGroup.execute('flushdb')
        then:
        reply == ErrorReply.READONLY

        when: 'fence off, slot writable: flush proceeds'
        localPersist.setRoleTransitionWriteFenced(false)
        reply = fGroup.execute('flushdb')
        then:
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result

        when: '76.7: key analysis store is cleared once even when firstOneSlot() resolves to nothing'
        ConfForGlobal.clusterEnabled = true
        def multiShard = localPersist.multiShard
        multiShard.mySelfShard().multiSlotRange.list.clear()
        localPersist.firstOneSlot() == null

        def keyDir = new File(Consts.persistDir, 'keys-for-analysis-fgroup')
        if (!keyDir.exists()) {
            keyDir.mkdirs()
        }
        def analysisEventloop = Eventloop.builder()
                .withIdleInterval(Duration.ofMillis(100))
                .build()
        analysisEventloop.keepAlive(true)
        Thread.start {
            analysisEventloop.run()
        }
        def countingHandler = new CountingKeyAnalysisHandler(keyDir, analysisEventloop, Config.create())
        def pool = new FlushCountingPool(countingHandler, (byte) 1, Consts.persistDir, Config.create())
        localPersist.indexHandlerPool = pool
        reply = fGroup.execute('flushdb')
        then:
        localPersist.firstOneSlot() == null
        reply instanceof AsyncReply
        (reply as AsyncReply).settablePromise.whenResult { result ->
            result == OKReply.INSTANCE
        }.result
        countingHandler.flushdbCalls == 1

        cleanup:
        ConfForGlobal.clusterEnabled = false
        localPersist.setRoleTransitionWriteFenced(false)
        oneSlot.setReadonly(false)
        if (countingHandler != null) {
            countingHandler.cleanUp()
        }
        if (analysisEventloop) {
            analysisEventloop.breakEventloop()
        }
        localPersist.cleanUp()
    }

    static class CountingKeyAnalysisHandler extends KeyAnalysisHandler {
        int flushdbCalls = 0

        CountingKeyAnalysisHandler(File keyDir, Eventloop eventloop, Config config) throws Exception {
            super(keyDir, eventloop, config)
        }

        @Override
        CompletableFuture<Void> flushdb() {
            flushdbCalls++
            return CompletableFuture.completedFuture(null)
        }
    }

    static class FlushCountingPool extends IndexHandlerPool {
        final CountingKeyAnalysisHandler countingHandler

        FlushCountingPool(CountingKeyAnalysisHandler handler, byte indexWorkers, File persistDir, Config config) throws IOException {
            super(indexWorkers, persistDir, config)
            this.countingHandler = handler
        }

        @Override
        KeyAnalysisHandler getKeyAnalysisHandler() {
            return countingHandler
        }
    }
}
