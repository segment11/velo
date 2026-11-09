package io.velo.command

import io.velo.BaseCommand
import io.velo.ConfForGlobal
import io.velo.ConfForSlot
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.persist.Mock
import io.velo.reply.*
import spock.lang.Specification

import java.util.concurrent.TimeUnit

class KGroupTest extends Specification {
    def _KGroup = new KGroup(null, null, null)
    final short slot = 0
    final short slot1 = 1

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]
        int slotNumber = 128

        and:
        data2[1] = 'a'.bytes

        when:
        def sList = _KGroup.parseSlots('kx', data2, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def kGroup = new KGroup(null, null, null)
        kGroup.from(BaseCommand.mockAGroup())

        expect:
        kGroup.execute(input) == expected

        where:
        input   | expected
        'keys'  | ErrorReply.FORMAT
        'zzz'   | NilReply.INSTANCE
    }

    def 'test keys'() {
        given:
        def kGroup = new KGroup(null, null, null)
        // slot number 2, writes are spread over 2 slots by key hash, keys() must aggregate both
        kGroup.from(BaseCommand.mockAGroup((byte) 0, (byte) 1, (short) 2))

        and:
        ConfForSlot.global.confBucket.initialSplitNumber = (byte) 1
        ConfForSlot.global.confBucket.bucketsPerSlot = 4096
        LocalPersistTest.prepareLocalPersist((byte) 1, (short) 2)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        localPersist.fixSlotThreadId(slot1, Thread.currentThread().threadId())

        when: 'redis glob patterns over keys living in the wal of both slots'
        ['user:1', 'user:2', 'user', 'userr', 'hello', 'hxllo', 'hllo', 'app.log', 'appXlog'].each {
            kGroup.set(it, it.bytes)
        }
        then:
        keysOf(kGroup, 'keys user:*') == ['user:1', 'user:2']
        keysOf(kGroup, 'keys user*') == ['user', 'user:1', 'user:2', 'userr']
        keysOf(kGroup, 'keys h?llo') == ['hello', 'hxllo']
        keysOf(kGroup, 'keys hllo') == ['hllo']
        keysOf(kGroup, 'keys h[ax]llo') == ['hxllo']
        keysOf(kGroup, 'keys h[eo]llo') == ['hello']
        keysOf(kGroup, 'keys app.log') == ['app.log']
        keysOf(kGroup, 'keys app?log') == ['app.log', 'appXlog']
        keysOf(kGroup, 'keys *') == ['app.log', 'appXlog', 'hello', 'hllo', 'hxllo', 'user', 'user:1', 'user:2', 'userr']
        keysOf(kGroup, 'keys user') == ['user']
        keysOf(kGroup, 'keys nonexistent') == []
        // unclosed char class is glob-legal, matched literally, no crash
        keysOf(kGroup, 'keys foo[') == []
        // empty pattern matches nothing
        kGroup.execute('keys _0') == MultiBulkReply.EMPTY
        kGroup.execute('keys a b') == ErrorReply.FORMAT

        when: 'del is visible because the real keyspace is iterated'
        def dGroup = new DGroup(null, null, null)
        dGroup.from(kGroup)
        dGroup.execute('del user:1')
        then:
        keysOf(kGroup, 'keys user*') == ['user', 'user:2', 'userr']

        when: 'expired key is not returned'
        def eGroup = new EGroup(null, null, null)
        eGroup.from(kGroup)
        eGroup.execute('expire user 1')
        Thread.sleep(1100)
        then:
        keysOf(kGroup, 'keys user') == []

        when: 'a key written in the same millisecond as the keys call must be returned, see bug 80 re-review 3'
        int sameMsMiss = 0
        20.times {
            def key = 'sms:' + it
            kGroup.set(key, 'v'.bytes)
            if (keysOf(kGroup, 'keys ' + key) != [key]) {
                sameMsMiss++
            }
        }
        then:
        sameMsMiss == 0

        when: 'result is capped by keys.matchMaxCount, default 100'
        150.times {
            kGroup.set('q:' + it, ('q:' + it).bytes)
        }
        then:
        keysOf(kGroup, 'keys q:*').size() == 100

        and: 'the cap does not starve high slots, fair round-robin share, see bug 80 re-review 3 S1'
        def slotOf = { String key -> BaseCommand.slot(key, 2).slot() }
        def cappedKeys = keysOf(kGroup, 'keys q:*')
        def keysInSlot0 = (0..<150).collect { 'q:' + it }.count { slotOf(it) == 0 }
        def keysInSlot1 = (0..<150).collect { 'q:' + it }.count { slotOf(it) == 1 }
        def fromSlot0 = cappedKeys.count { slotOf(it) == 0 }
        def fromSlot1 = cappedKeys.count { slotOf(it) == 1 }
        fromSlot0 + fromSlot1 == 100
        fromSlot0 > 0 && fromSlot1 > 0
        // fair share: near-equal, unless the smaller slot is exhausted
        Math.abs(fromSlot0 - fromSlot1) <= 1 || Math.min(fromSlot0, fromSlot1) == Math.min(keysInSlot0, keysInSlot1)

        when: 'the cap knob is registered in dyn config and settable'
        localPersist.oneSlot(slot).updateDynConfig(KGroup.MATCH_MAX_COUNT_KEY, '200')
        localPersist.oneSlot(slot1).updateDynConfig(KGroup.MATCH_MAX_COUNT_KEY, '200')
        then:
        keysOf(kGroup, 'keys q:*').size() == 150

        when: 'the knob is global, setting it on any slot is honored, see bug 80 re-review 3 S2'
        localPersist.oneSlot(slot).updateDynConfig(KGroup.MATCH_MAX_COUNT_KEY, '100')
        localPersist.oneSlot(slot1).updateDynConfig(KGroup.MATCH_MAX_COUNT_KEY, '200')
        then:
        keysOf(kGroup, 'keys q:*').size() == 150

        when: 'invalid knob value is rejected'
        localPersist.oneSlot(slot).updateDynConfig(KGroup.MATCH_MAX_COUNT_KEY, 'abc')
        then:
        thrown(IllegalArgumentException)

        when: 'keys persisted directly into key buckets while the wal has no such keys'
        def shortValueList = Mock.prepareShortValueList(10)
        localPersist.oneSlot(slot).keyLoader.persistShortValueListBatchInOneWalGroup(0, shortValueList)
        then:
        keysOf(kGroup, 'keys key:*') == (0..<10).collect { 'key:' + it.toString().padLeft(12, '0') }.sort()

        when: 'one scan call can be cut short by onceScanMaxLoopCount, keys() must resume via the cursor'
        ConfForSlot.global.confBucket.onceScanMaxLoopCount = 1
        localPersist.oneSlot(slot).keyLoader.flush()
        localPersist.oneSlot(slot).keyLoader.persistShortValueListBatchInOneWalGroup(0, Mock.prepareShortValueList(10, 0))
        then:
        keysOf(kGroup, 'keys key:*') == (0..<10).collect { 'key:' + it.toString().padLeft(12, '0') }.sort()

        cleanup:
        ConfForSlot.global.confBucket.onceScanMaxLoopCount = 1024
        ConfForGlobal.keysMatchMaxCount = 100
        localPersist.cleanUp()
        localPersist.resetForTest()
        Consts.persistDir.deleteDir()
    }

    private static List<String> keysOf(KGroup kGroup, String line) {
        def reply = kGroup.execute(line)
        assert reply instanceof AsyncReply
        def result = (reply as AsyncReply).settablePromise.toCompletableFuture().get(10, TimeUnit.SECONDS) as MultiBulkReply
        return result.replies.collect { new String((it as BulkReply).raw) }.sort()
    }
}
