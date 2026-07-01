package io.velo.repl

import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.repl.incremental.XFlush
import io.velo.repl.incremental.XWalV
import spock.lang.Specification

class ScaleUpReplicationTest extends Specification {
    def 'test scale-up 2N replay: routing, gate, and flush rejection'() {
        given:
        def savedSlotNumber = ConfForGlobal.slotNumber
        def savedMasterSlotNumber = ConfForGlobal.masterSlotNumber

        // master N=2, slave 2N=4
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 4)
        def localPersist = LocalPersist.instance
        for (short s = 0; s < 4; s++) {
            localPersist.fixSlotThreadId(s, Thread.currentThread().threadId())
        }

        ConfForGlobal.slotNumber = (short) 4
        ConfForGlobal.masterSlotNumber = (short) 2
        localPersist.resetScaleUpReadGate(2)

        // init all slots to readonly slaves with canRead=false
        for (short s = 0; s < 4; s++) {
            localPersist.oneSlot(s).readonly = true
            localPersist.oneSlot(s).canRead = false
        }

        and:
        def replPair = ReplPairTest.mockAsSlave()

        // Use Mock to get a Wal.V; find its 2N(=4) target slot.
        // The stream slot is 0 (first master stream). We need a key whose target != 0 to prove cross-slot fanout.
        def v0 = null
        short streamSlot = 0
        for (int i = 0; i < 200; i++) {
            def vList = Mock.prepareValueList(1, 0) { v ->
                // adjust bucketIndex to match the 2N target slot
                def s = BaseCommand.slot(v.key(), 4)
                return new io.velo.persist.Wal.V(v.seq(), s.bucketIndex(), v.keyHash(), v.expireAt(),
                        v.spType(), v.key(), v.cvEncoded(), false)
            }
            def candidate = vList[0]
            def candidateTargetSlot = BaseCommand.slot(candidate.key(), 4).slot()
            if (candidateTargetSlot != streamSlot) {
                v0 = candidate
                break
            }
        }
        assert v0 != null : 'found a key whose 2N target differs from the stream slot'

        def targetSlotInfo = BaseCommand.slot(v0.key(), 4)
        short targetSlot = targetSlotInfo.slot()

        when: 'apply XWalV for a key targeting a different slot via applyAsync (cross-slot fanout)'
        def xWalV = new XWalV(v0, false)
        def promise = xWalV.applyAsync(streamSlot, replPair)
        promise.getResult()
        then: 'the data lands in the correct target slot'
        promise.isComplete()
        !promise.isException()
        targetSlot != streamSlot
        localPersist.oneSlot(targetSlot).get(v0.key(), targetSlotInfo.bucketIndex(), targetSlotInfo.keyHash()) != null

        when: 'gate is closed — no slot should be readable'
        then:
        !localPersist.oneSlot((short) 0).canRead
        !localPersist.oneSlot((short) 3).canRead

        when: 'only stream 0 ready — gate still closed'
        localPersist.publishStreamReadyAndRefreshGate((short) 0, true)
        then:
        !localPersist.oneSlot((short) 0).canRead

        when: 'stream 1 also ready — gate opens, all 4 slots readable'
        localPersist.publishStreamReadyAndRefreshGate((short) 1, true)
        then:
        localPersist.oneSlot((short) 0).canRead
        localPersist.oneSlot((short) 3).canRead

        when: 'XFlush is rejected in scale-up mode (stuck-but-safe)'
        new XFlush().applyAsync(streamSlot, replPair)
        then:
        thrown(IllegalStateException)

        when: 'XFlush.apply also rejected (defense-in-depth)'
        new XFlush().apply(streamSlot, replPair)
        then:
        thrown(IllegalStateException)

        and: 'after a rejected flush, the gate stays closed — reads are blocked (stuck-but-safe outcome)'
        // Close the gate first (drop a stream) then try flush; the gate must NOT reopen via flush
        localPersist.publishStreamReadyAndRefreshGate((short) 0, false)
        !localPersist.oneSlot((short) 0).canRead
        !localPersist.oneSlot((short) 3).canRead

        and: 'the fetched-offset is unchanged — the flush did not silently advance progress'
        def offsetBefore = localPersist.oneSlot(streamSlot).getMetaChunkSegmentIndex().getMasterBinlogFileIndexAndOffset()
        try {
            new XFlush().applyAsync(streamSlot, replPair)
        } catch (IllegalStateException ignored) {
        }
        def offsetAfter = localPersist.oneSlot(streamSlot).getMetaChunkSegmentIndex().getMasterBinlogFileIndexAndOffset()
        offsetAfter.fileIndex() == offsetBefore.fileIndex()
        offsetAfter.offset() == offsetBefore.offset()

        cleanup:
        ConfForGlobal.slotNumber = savedSlotNumber
        ConfForGlobal.masterSlotNumber = savedMasterSlotNumber
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
