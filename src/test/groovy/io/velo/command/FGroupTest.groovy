package io.velo.command

import io.velo.BaseCommand
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.Binlog
import io.velo.reply.*
import spock.lang.Specification

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
        def fGroup = new FGroup(cmd, data2, null)
        fGroup.from(BaseCommand.mockAGroup())

        expect:
        fGroup.handle() == expected

        where:
        cmd        | expected
        'failover' | OKReply.INSTANCE
        'flushall' | OKReply.INSTANCE
        'flushdb'  | OKReply.INSTANCE
        'zzz'      | NilReply.INSTANCE
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

        cleanup:
        localPersist.setRoleTransitionWriteFenced(false)
        oneSlot.setReadonly(false)
        localPersist.cleanUp()
    }
}
