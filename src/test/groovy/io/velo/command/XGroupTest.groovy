package io.velo.command

import io.activej.eventloop.Eventloop
import io.netty.buffer.Unpooled
import io.velo.*
import io.velo.persist.*
import io.velo.repl.*
import io.velo.repl.Repl.ReplReply
import io.velo.repl.content.Hello
import io.velo.repl.content.Hi
import io.velo.repl.content.Ping
import io.velo.repl.content.Pong
import io.velo.repl.incremental.XWalV
import io.velo.reply.AsyncReply
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.NilReply
import spock.lang.Specification

import java.nio.ByteBuffer
import java.time.Duration

class XGroupTest extends Specification {
    def _XXGroup = new XGroup(null, null, null)

    final short slot = 0
    final short slotNumber = 2

    private byte[][] mockData(ReplPair replPair, ReplType replType, ReplContent content) {
        def reply = Repl.reply(slot, replPair, replType, content)
        mockData(reply)
    }

    private byte[][] mockData(ReplReply reply) {
        def nettyBuf = Unpooled.wrappedBuffer(reply.buffer().array())
        Repl.decode(nettyBuf)
    }

    def 'test parse slot'() {
        given:
        def data2 = new byte[2][]

        and:
        data2[1] = 'sub_cmd'.bytes

        when:
        def sList = _XXGroup.parseSlots('x_repl', data2, slotNumber)
        then:
        sList.size() == 0

        when:
        def data1 = new byte[1][]
        sList = _XXGroup.parseSlots('x_repl', data1, slotNumber)
        then:
        sList.size() == 0

        when:
        def data4 = new byte[4][]
        data4[1] = 'slot'.bytes
        data4[2] = '0'.bytes
        data4[3] = 'sub_cmd'.bytes
        sList = _XXGroup.parseSlots('x_repl', data4, slotNumber)
        then:
        sList.size() == 1

        when:
        data4[2] = 'a'.bytes
        sList = _XXGroup.parseSlots('x_repl', data4, slotNumber)
        then:
        // invalid int
        sList.size() == 0

        when:
        def data3 = new byte[3][]
        data3[1] = 'slot'.bytes
        data3[2] = '0'.bytes
        sList = _XXGroup.parseSlots('x_repl', data3, slotNumber)
        then:
        sList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def xGroup = new XGroup('x_repl', data1, null)
        xGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = xGroup.handle()
        then:
        reply == NilReply.INSTANCE

        when:
        def data4 = new byte[4][]
        data4[1] = 'slot'.bytes
        data4[2] = '0'.bytes
        data4[3] = 'xxx'.bytes
        xGroup.data = data4
        xGroup.slotWithKeyHashListParsed = _XXGroup.parseSlots('x_repl', data4, slotNumber)
        reply = xGroup.handle()
        then:
        reply == NilReply.INSTANCE

        when:
        def data3 = new byte[3][]
        data3[1] = 'slot'.bytes
        data3[2] = '0'.bytes
        xGroup.data = data3
        xGroup.slotWithKeyHashListParsed = _XXGroup.parseSlots('x_repl', data3, slotNumber)
        reply = xGroup.handle()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def data2 = new byte[2][]
        data2[1] = XGroup.X_CONF_FOR_SLOT_AS_SUB_CMD.bytes
        xGroup.data = data2
        xGroup.slotWithKeyHashListParsed = _XXGroup.parseSlots('x_repl', data2, slotNumber)
        reply = xGroup.handle()
        then:
        reply instanceof BulkReply
    }

    def 'test handle2'() {
        given:
        def data4 = new byte[4][]
        data4[1] = 'slot'.bytes
        data4[2] = '0'.bytes
        data4[3] = XGroup.X_GET_FIRST_SLAVE_LISTEN_ADDRESS_AS_SUB_CMD.bytes

        def xGroup = new XGroup('x_repl', data4, null)
        xGroup.from(BaseCommand.mockAGroup())
        xGroup.slotWithKeyHashListParsed = _XXGroup.parseSlots('x_repl', data4, slotNumber)

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def reply = xGroup.handle()
        then:
        reply == NilReply.INSTANCE

        when:
        oneSlot.createIfNotExistReplPairAsMaster(11L, 'localhost', 6380)
        reply = xGroup.handle()
        then:
        reply instanceof BulkReply
        new String(((BulkReply) reply).raw) == 'localhost:6380'

        when:
        data4[3] = 'x_catch_up'.bytes
        reply = xGroup.handle()
        then:
        reply == ErrorReply.SYNTAX

        when:
        xGroup.slotWithKeyHashListParsed = []
        reply = xGroup.handle()
        then:
        reply == ErrorReply.SYNTAX

        when:
        def binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        def data9 = new byte[9][]
        data9[1] = 'slot'.bytes
        data9[2] = '0'.bytes
        data9[3] = 'x_catch_up'.bytes
        data9[4] = '10'.bytes
        data9[5] = oneSlot.masterUuid.toString().bytes
        data9[6] = '0'.bytes
        data9[7] = binlogOneSegmentLength.toString().bytes
        data9[8] = '0'.bytes
        xGroup.data = data9
        xGroup.slotWithKeyHashListParsed = _XXGroup.parseSlots('x_repl', data9, slotNumber)
        reply = xGroup.handle()
        then:
        reply instanceof BulkReply

        when:
        def replPairAsMaster = oneSlot.firstReplPairAsMaster
        oneSlot.replPairAsMasterList
        replPairAsMaster.bye()
        reply = xGroup.handle()
        then:
        reply instanceof BulkReply

        when:
        data9[5] = '0'.bytes
        reply = xGroup.handle()
        then:
        reply instanceof ErrorReply

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test as master'() {
        given:
        def data4 = new byte[4][]
        // slave uuid long
        data4[0] = new byte[8]
        // slot
        data4[1] = new byte[2]
        ByteBuffer.wrap(data4[1]).putShort(slot)
        // repl type
        data4[2] = new byte[1]
        // no exist repl type
        data4[2][0] = (byte) -10

        def xGroup = new XGroup(null, data4, null)

        expect:
        _XXGroup.parseSlots(null, data4, slotNumber).size() == 0
        // invalid repl type
        xGroup.handleRepl() == null

        when:
        ConfForGlobal.netListenAddresses = 'localhost:6379'

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        // mock from slave repl request data
        final long slaveUuid = 1L
        def replPairAsSlave = ReplPairTest.mockAsSlave(0L, slaveUuid)
        def ping = new Ping('localhost:6380')
        def data = mockData(replPairAsSlave, ReplType.ping, ping)

        def x = new XGroup(null, data, null)
        ReplReply r = x.handleRepl()
        then:
        r.isReplType(ReplType.pong)

        when:
        // handle ping again, already created repl pair as master when first received ping
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.pong)

        // hello
        when:
        def hello = new Hello(slaveUuid, 'localhost:6380')
        data = mockData(replPairAsSlave, ReplType.hello, hello)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.hi)
        oneSlot.getReplPairAsMaster(slaveUuid) != null
        oneSlot.dynConfig.binlogOn

        when:
        x.replPair = null
        r = x.hello(slot, data[3])
        then:
        r.isReplType(ReplType.hi)

        // bye
        when:
        data = mockData(replPairAsSlave, ReplType.bye, ping)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.byeBye)

        when:
        // remove repl pair
        oneSlot.doTask(0)
        x.replPair = null
        def replPairAsMaster = oneSlot.firstReplPairAsMaster
        oneSlot.replPairAsMasterList
        replPairAsMaster.bye()
        r = x.handleRepl()
        then:
        // empty
        r.isEmpty()

        when:
        replPairAsMaster.sendBye = false
        // master receive hello from slave, then create repl pair again
        data = mockData(replPairAsSlave, ReplType.hello, hello)
        x = new XGroup(null, data, null)
        x.handleRepl()
        ByteBuffer.wrap(data4[0]).putLong(slaveUuid)
        // response exists chunk segments
        data4[2][0] = ReplType.exists_chunk_segments.code

        def metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(0, FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        def contentBytes = new byte[4 + 4 + metaBytes.length]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        requestBuffer.put(metaBytes)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_chunk_segments)
        // skip as meta bytes is same
        r.buffer().limit() == Repl.HEADER_LENGTH + 8

        when:
        // meta bytes not same
        requestBuffer.put(8, (byte) 1)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_chunk_segments)
        // only meta bytes, chunk segment bytes not write yet
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 8 + FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD * MetaChunkSegmentFlagSeq.ONE_LENGTH

        when:
        // chunk segment bytes exists
        oneSlot.chunk.writeSegmentToTargetSegmentIndex(new byte[4096], 0)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_chunk_segments)
        // meta bytes with just one chunk segment bytes
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 8 + FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD * MetaChunkSegmentFlagSeq.ONE_LENGTH + 4096

        // response exists wal
        when:
        data4[2][0] = ReplType.exists_wal.code
        contentBytes = new byte[4 + 8 + 8]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // wal group index
        requestBuffer.putInt(0)
        requestBuffer.putLong(0L)
        requestBuffer.putLong(0L)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // skip
        r.isReplType(ReplType.s_exists_wal)

        when:
        requestBuffer.position(0)
        // no log, skip
        requestBuffer.putInt(99)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_wal)

        when:
        requestBuffer.position(4)
        requestBuffer.putLong(1L)
        r = x.handleRepl()
        then:
        // no skip
        r.isReplType(ReplType.s_exists_wal)

        when:
        requestBuffer.position(4)
        requestBuffer.putLong(0L)
        requestBuffer.putLong(1L)
        r = x.handleRepl()
        then:
        // no skip
        r.isReplType(ReplType.s_exists_wal)

        when:
        requestBuffer.position(4)
        requestBuffer.putLong(1L)
        requestBuffer.putLong(1L)
        r = x.handleRepl()
        then:
        // no skip
        r.isReplType(ReplType.s_exists_wal)

        when:
        requestBuffer.position(0)
        // no log, no skip
        requestBuffer.putInt(99)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_wal)

        when:
        requestBuffer.position(0)
        // trigger log, no skip
        requestBuffer.putInt(100)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_wal)

        when:
        // exception
        def walGroupNumber = Wal.calcWalGroupNumber()
        requestBuffer.position(0)
        requestBuffer.putInt(walGroupNumber)
        r = x.handleRepl()
        then:
        // error
        r.isReplType(ReplType.error)

        when:
        requestBuffer.position(0)
        requestBuffer.putInt(-1)
        r = x.handleRepl()
        then:
        // error
        r.isReplType(ReplType.error)

        // response exists key buckets
        when:
        data4[2][0] = ReplType.exists_key_buckets.code
        contentBytes = new byte[1 + 4 + 8]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // split index
        requestBuffer.put((byte) 0)
        // begin bucket index
        requestBuffer.putInt(0)
        // one wal group seq
        requestBuffer.putLong(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_key_buckets)
        // refer XGroup method exists_key_buckets
        r.buffer().limit() == Repl.HEADER_LENGTH + 1 + 1 + 4 + 1 + 8

        when:
        // one wal group seq not match
        requestBuffer.putLong(1 + 4, -1L)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_key_buckets)
        // key buckets not exists
        r.buffer().limit() == Repl.HEADER_LENGTH + 1 + 1 + 4 + 1 + 8

        when:
        def sharedBytesList = new byte[1][]
        sharedBytesList[0] = new byte[4096 * ConfForSlot.global.confWal.oneChargeBucketNumber]
        oneSlot.keyLoader.writeSharedBytesList(sharedBytesList, 0)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_key_buckets)
        // key buckets exists
        r.buffer().limit() == Repl.HEADER_LENGTH + 1 + 1 + 4 + 1 + 8 + sharedBytesList[0].length

        // stat_key_count_in_buckets
        when:
        data4[2][0] = ReplType.stat_key_count_in_buckets.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_stat_key_count_in_buckets)
        r.buffer().limit() == Repl.HEADER_LENGTH + ConfForSlot.global.confBucket.bucketsPerSlot * 2

        // meta_key_bucket_split_number
        when:
        data4[2][0] = ReplType.meta_key_bucket_split_number.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_meta_key_bucket_split_number)
        r.buffer().limit() == Repl.HEADER_LENGTH + ConfForSlot.global.confBucket.bucketsPerSlot

        // incremental_big_string
        when:
        data4[2][0] = ReplType.incremental_big_string.code
        // big string uuid long
        contentBytes = new byte[8]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_incremental_big_string)
        r.buffer().limit() == Repl.HEADER_LENGTH + 8

        when:
        def bigStringUuid = 1L
        oneSlot.bigStringFiles.writeBigStringBytes(bigStringUuid, 'test-big-string-key', new byte[1024])
        ByteBuffer.wrap(contentBytes).putLong(bigStringUuid)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_incremental_big_string)
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 1024

        // exists_big_string
        when:
        data4[2][0] = ReplType.exists_big_string.code
        contentBytes = new byte[1]
        data4[3] = contentBytes
        // master has one big string
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_big_string)

        when:
        oneSlot.bigStringFiles.deleteBigStringFileIfExist(bigStringUuid)
        // master has no big string
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_big_string)

        when:
        contentBytes = new byte[8 * 2]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(1L)
        requestBuffer.putLong(2L)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_big_string)

        // exists_dict
        when:
        data4[2][0] = ReplType.exists_dict.code
        contentBytes = new byte[1]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_dict)

        when:
        contentBytes = new byte[4 * 2]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(1)
        requestBuffer.putInt(2)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_dict)

        // exists_all_done
        when:
        data4[2][0] = ReplType.exists_all_done.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_exists_all_done)

        // catch_up
        when:
        data4[2][0] = ReplType.catch_up.code
        contentBytes = new byte[8 + 4 + 8 + 8]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // master uuid long
        // not match
        requestBuffer.putLong(oneSlot.masterUuid + 1)
        // binlog file index
        requestBuffer.putInt(0)
        // binlog file offset
        requestBuffer.putLong(0)
        // last updated file offset
        requestBuffer.putLong(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.error)

        when:
        requestBuffer.position(0)
        requestBuffer.putLong(oneSlot.masterUuid)
        r = x.handleRepl()
        then:
        // empty content return
        r.isReplType(ReplType.s_catch_up)

        when:
        oneSlot.readonly = true
        r = x.handleRepl()
        then:
        // empty content return
        r.isReplType(ReplType.s_catch_up)

        when:
        def vList = Mock.prepareValueList(10)
        for (v in vList) {
            oneSlot.appendBinlog(new XWalV(v))
        }
        // read segment bytes < one segment length
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8 + 4 + 8)
        requestBuffer.putLong(106)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8 + 4 + 8)
        requestBuffer.putLong(oneSlot.binlog.currentFileIndexAndOffset().offset())
        r = x.handleRepl()
        then:
        // empty content return
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8)
        // file index not match master binlog current file index, read fail
        requestBuffer.putInt(1)
        requestBuffer.putLong(0)
        requestBuffer.putLong(oneSlot.binlog.currentFileIndexAndOffset().offset())
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.error)

        when:
        // mock as binlog file index match
        def binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        def binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength
        (binlogOneFileMaxLength / binlogOneSegmentLength).intValue().times {
            oneSlot.binlog.moveToNextSegment(true)
        }
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(0)
        requestBuffer.putLong(oneSlot.masterUuid)
        requestBuffer.putInt(0)
        // not margin
        requestBuffer.putLong(1)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.error)

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test as slave'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:6380'
        ConfForSlot.global.confChunk.REPL_EMPTY_BYTES_FOR_ONCE_WRITE = new byte[FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD * 4096]

        LocalPersistTest.prepareLocalPersist((byte) 1, slotNumber)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        // mock from master repl response data
        final long masterUuid = 10L
        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid
        def pong = new Pong('localhost:6379')
        def data = mockData(replPairAsMaster, ReplType.pong, pong)
        def x = new XGroup(null, data, null)
        x.replPair = null
        ReplReply r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        oneSlot.createReplPairAsSlave('localhost', 6379)
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        // error
        data = mockData(Repl.error(slot, replPairAsMaster, 'error'))
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isEmpty()

        // hi
        when:
        def metaChunkSegmentIndex = oneSlot.metaChunkSegmentIndex
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false, 0, 0L)
        def hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        data = mockData(replPairAsMaster, ReplType.hi, hi)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.exists_dict)

        when:
        // first fetch dict, send local exist dict seq
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.persistDir)
        dictMap.putDict('key:', new Dict(new byte[10]))
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.exists_dict)

        when:
        // already fetch all exists data, just catch up binlog
        // from beginning
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // last updated offset not margined
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 106L)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // last updated offset margined
        def binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, binlogOneSegmentLength)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // not match slave uuid
        hi = new Hi(replPairAsMaster.slaveUuid + 1, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        data = mockData(replPairAsMaster, ReplType.hi, hi)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r == null

        // ok
        when:
        data = mockData(Repl.test(slot, replPairAsMaster, 'test'))
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isEmpty()

        // byeBye
        when:
        data = mockData(replPairAsMaster, ReplType.byeBye, pong)
        x = new XGroup(null, data, null)
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        def data4 = new byte[4][]
        // slave uuid long
        data4[0] = new byte[8]
        ByteBuffer.wrap(data4[0]).putLong(replPairAsMaster.slaveUuid)
        // slot
        data4[1] = new byte[2]
        ByteBuffer.wrap(data[1]).putShort(slot)
        // repl type
        data4[2] = new byte[1]
        // fetch exists chunk segments
        data4[2][0] = ReplType.s_exists_chunk_segments.code

        def contentBytes = new byte[8]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_chunk_segments)

        when:
        // last batch
        requestBuffer.position(0)
        requestBuffer.putInt(ConfForSlot.global.confChunk.maxSegmentNumber() - FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.exists_wal)

        when:
        requestBuffer.position(0)
        // next batch will delay run
        requestBuffer.putInt(FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD * 9)
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        def metaBytes = oneSlot.getMetaChunkSegmentFlagSeq().getOneBatch(0, FdReadWrite.REPL_ONCE_SEGMENT_COUNT_PREAD)
        contentBytes = new byte[8 + 4 + metaBytes.length + 4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(1024)
        requestBuffer.putInt(metaBytes.length)
        requestBuffer.put(metaBytes)
        requestBuffer.putInt(0)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_chunk_segments)

        when:
        contentBytes = new byte[8 + 4 + metaBytes.length + 4 + 4096]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(1024)
        requestBuffer.putInt(metaBytes.length)
        requestBuffer.put(metaBytes)
        requestBuffer.putInt(4096)
        data4[3] = contentBytes
        x = new XGroup(null, data4, null)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_chunk_segments)

        // exists wal
        when:
        data4[2][0] = ReplType.s_exists_wal.code
        contentBytes = new byte[16 + 2 * Wal.ONE_GROUP_BUFFER_SIZE]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // wal group index
        requestBuffer.putInt(0)
        requestBuffer.putInt(Wal.ONE_GROUP_BUFFER_SIZE)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_wal)

        when:
        // skip, no log
        contentBytes = new byte[4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(99)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // delay fetch next batch
        r.isEmpty()

        when:
        // skip, trigger log
        contentBytes = new byte[4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(100)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_wal)

        when:
        // last batch
        def walGroupNumber = Wal.calcWalGroupNumber()
        requestBuffer.position(0)
        requestBuffer.putInt(walGroupNumber - 1)
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_reverse_index)

        // fetch exists key buckets
        when:
        data4[2][0] = ReplType.s_exists_key_buckets.code
        contentBytes = new byte[1 + 1 + 4 + 1 + 8]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // split index
        requestBuffer.put((byte) 0)
        // max split number
        requestBuffer.put((byte) 1)
        // begin bucket index
        requestBuffer.putInt(0)
        // is skip flag
        requestBuffer.put((byte) 1)
        // one wal group seq
        requestBuffer.putLong(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_key_buckets)

        when:
        // is skip flag false
        requestBuffer.put(1 + 1 + 4, (byte) 0)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_key_buckets)

        when:
        contentBytes = new byte[1 + 1 + 4 + 1 + 8 + 4096 * ConfForSlot.global.confWal.oneChargeBucketNumber]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // split index
        requestBuffer.put((byte) 0)
        // max split number
        requestBuffer.put((byte) 1)
        // begin bucket index
        requestBuffer.putInt(0)
        // is skip flag
        requestBuffer.put((byte) 0)
        // one wal group seq
        requestBuffer.putLong(0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_key_buckets)

        when:
        // last batch in split index 0
        // max split number
        requestBuffer.put(1, (byte) 3)
        requestBuffer.putInt(1 + 1, ConfForSlot.global.confBucket.bucketsPerSlot - ConfForSlot.global.confWal.oneChargeBucketNumber)
        r = x.handleRepl()
        then:
        // delay fetch next batch
        r.isEmpty()

        when:
        // last batch in split index 2
        requestBuffer.put(0, (byte) 2)
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_chunk_segments)

        // s_stat_key_count_in_buckets
        when:
        data4[2][0] = ReplType.s_stat_key_count_in_buckets.code
        contentBytes = new byte[ConfForSlot.global.confBucket.bucketsPerSlot * 2]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_key_buckets)

        // s_meta_key_bucket_split_number
        when:
        data4[2][0] = ReplType.s_meta_key_bucket_split_number.code
        contentBytes = new byte[ConfForSlot.global.confBucket.bucketsPerSlot]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.stat_key_count_in_buckets)

        // s_incremental_big_string
        when:
        data4[2][0] = ReplType.s_incremental_big_string.code
        contentBytes = new byte[8]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        contentBytes = new byte[8 + 1024]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(1L)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()
        oneSlot.bigStringFiles.getBigStringBytes(1L).length == 1024

        // s_exists_big_string
        when:
        data4[2][0] = ReplType.s_exists_big_string.code
        contentBytes = new byte[1]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.meta_key_bucket_split_number)

        when:
        contentBytes = new byte[2 + 1]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // big string count short
        requestBuffer.putShort((short) 0)
        // is sent all once flag
        requestBuffer.put((byte) 1)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.meta_key_bucket_split_number)

        when:
        // mock two big string fetched
        contentBytes = new byte[2 + 1 + (8 + 4 + 1024) * 2]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putShort((short) 2)
        requestBuffer.put((byte) 1)
        requestBuffer.putLong(1L)
        requestBuffer.putInt(1024)
        requestBuffer.position(requestBuffer.position() + 1024)
        requestBuffer.putLong(2L)
        requestBuffer.putInt(1024)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.meta_key_bucket_split_number)

        when:
        // is sent all false
        requestBuffer.put(2, (byte) 0)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.exists_big_string)

        when:
        def bigStringFileUuidList = oneSlot.bigStringFiles.bigStringFileUuidList
        if (bigStringFileUuidList) {
            for (uuid in bigStringFileUuidList) {
                oneSlot.bigStringFiles.deleteBigStringFileIfExist(uuid)
            }
        }
        r = x.fetchExistsBigString(slot, oneSlot)
        then:
        // empty content return
        r.isReplType(ReplType.exists_big_string)

        // s_exists_dict
        when:
        data4[2][0] = ReplType.s_exists_dict.code
        // dict count int
        contentBytes = new byte[4]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_big_string)

        when:
        def dict1 = new Dict(new byte[10])
        def dict2 = new Dict(new byte[20])
        def encoded1 = dict1.encode('k1')
        def encoded2 = dict2.encode(Dict.GLOBAL_ZSTD_DICT_KEY)
        contentBytes = new byte[4 + 4 + encoded1.length + 4 + encoded2.length]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(2)
        requestBuffer.putInt(encoded1.length)
        requestBuffer.put(encoded1)
        requestBuffer.putInt(encoded2.length)
        requestBuffer.put(encoded2)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.exists_big_string)
        dictMap.getDict('k1') != null
        Dict.GLOBAL_ZSTD_DICT.hasDictBytes()

        // s_exists_all_done
        when:
        data4[2][0] = ReplType.s_exists_all_done.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next step
        r.isReplType(ReplType.catch_up)
        oneSlot.metaChunkSegmentIndex.isExistsDataAllFetched()

        // s_catch_up
        when:
        // clear slave catch up binlog file index and offset
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        data4[2][0] = ReplType.s_catch_up.code
        // mock 10 wal values in binlog
        int n = 0
        def vList = Mock.prepareValueList(10)
        for (v in vList) {
            n += new XWalV(v).encodedLength()
        }
        contentBytes = new byte[1 + 4 + 8 + 4 + 8 + 4 + n]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // is readonly flag
        requestBuffer.put((byte) 0)
        // response binlog file index
        requestBuffer.putInt(0)
        // response binlog file offset
        requestBuffer.putLong(0)
        // current(latest) binlog file index
        requestBuffer.putInt(0)
        // current(latest) binlog file offset
        requestBuffer.putLong(n)
        // one segment bytes response
        requestBuffer.putInt(n)
        for (v in vList) {
            def encoded = new XWalV(v).encodeWithType()
            requestBuffer.put(encoded)
        }
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        contentBytes = new byte[1 + 4 + 8 + 4 + 8 + 4 + binlogOneSegmentLength]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.put((byte) 0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneSegmentLength * 2)
        requestBuffer.putInt(binlogOneSegmentLength)
        int mockSkipN = 0
        for (v in vList) {
            def encoded = new XWalV(v).encodeWithType()
            mockSkipN = encoded.length
            requestBuffer.put(encoded)
        }
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.catch_up)
        oneSlot.metaChunkSegmentIndex.masterBinlogFileIndexAndOffset.fileIndex() == 0
        oneSlot.metaChunkSegmentIndex.masterBinlogFileIndexAndOffset.offset() == binlogOneSegmentLength

        when:
        requestBuffer.position(1)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneSegmentLength)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.catch_up)

        when:
        requestBuffer.position(1)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneSegmentLength - 1)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, mockSkipN)
        r = x.handleRepl()
        then:
        // delay to fetch next batch
        r.isEmpty()

        when:
        requestBuffer.position(1)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.catch_up)

        when:
        // master readonly
        contentBytes[0] = (byte) 1
        r = x.handleRepl()
        then:
        // next batch
        r.isReplType(ReplType.catch_up)

        when:
        def binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength
        requestBuffer.position(1)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneFileMaxLength - binlogOneSegmentLength)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        r = x.handleRepl()
        then:
        // begin with new binlog file next batch, delay
        r.isEmpty()

        when:
        requestBuffer.position(1)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(1)
        requestBuffer.put((byte) 0)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        r = x.handleRepl()
        then:
        r.isReplType(ReplType.error)

        when:
        // not slot 0 catch up, wait slot 0
        localPersist.asSlaveSlot0FetchedExistsAllDone = false
        // slot 1
        ByteBuffer.wrap(data4[1]).putShort((short) 1)
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        // only readonly flag from master, mean no more binlog bytes
        contentBytes = new byte[1]
        contentBytes[0] = (byte) 1
        ByteBuffer.wrap(data4[1]).putShort((short) 0)
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()
        oneSlot.onlyOneReplPairAsSlave.allCaughtUp && oneSlot.onlyOneReplPairAsSlave.masterReadonly

        when:
        // only readonly flag from master, mean no more binlog bytes
        contentBytes[0] = (byte) 0
        r = x.handleRepl()
        then:
        r.isEmpty()
        oneSlot.onlyOneReplPairAsSlave.allCaughtUp && !oneSlot.onlyOneReplPairAsSlave.masterReadonly

        when:
        // skip reset readonly flag from master
        contentBytes = new byte[2]
        data4[3] = contentBytes
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        // pong, trigger catch up
        data4[2][0] = ReplType.pong.code
        contentBytes = new byte[0]
        data4[3] = contentBytes
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false, 0, 0L)
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        // pong, trigger catch up
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        oneSlot.onlyOneReplPairAsSlave.lastGetCatchUpResponseMillis = 0
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        // need no trigger catch up
        oneSlot.onlyOneReplPairAsSlave.lastGetCatchUpResponseMillis = System.currentTimeMillis() - 1000
        r = x.handleRepl()
        then:
        r.isEmpty()

        when:
        // trigger catch up
        oneSlot.onlyOneReplPairAsSlave.lastGetCatchUpResponseMillis = System.currentTimeMillis() - 10000
        r = x.handleRepl()
        then:
        r.isEmpty()

        cleanup:
        localPersist.cleanUp()
        dictMap.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test try catch up again after slave tcp client closed'() {
        given:
        def replPair = ReplPairTest.mockAsSlave()

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        when:
        def metaChunkSegmentIndex = oneSlot.metaChunkSegmentIndex
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(replPair.masterUuid, false, 0, 0L)
        XGroup.skipTryCatchUpAgainAfterSlaveTcpClientClosed = false
        XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(replPair)
        then:
        1 == 1

        when:
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(replPair.masterUuid + 1, true, 0, 0L)
        XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(replPair)
        then:
        1 == 1

        when:
        // mock 10 wal values in binlog
        int n = 0
        def vList = Mock.prepareValueList(10)
        for (v in vList) {
            n += new XWalV(v).encodedLength()
        }
        def contentBytes = new byte[1 + 4 + 8 + 4 + 8 + 4 + n]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        // is readonly flag
        requestBuffer.put((byte) 0)
        // response binlog file index
        requestBuffer.putInt(0)
        // response binlog file offset
        requestBuffer.putLong(0)
        // current(latest) binlog file index
        requestBuffer.putInt(0)
        // current(latest) binlog file offset
        requestBuffer.putLong(n)
        // one segment bytes response
        requestBuffer.putInt(n)
        for (v in vList) {
            def encoded = new XWalV(v).encodeWithType()
            requestBuffer.put(encoded)
        }
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(replPair.masterUuid, true, 0, 0L)
        XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(replPair, contentBytes)
        then:
        1 == 1

        when:
        XGroup.skipTryCatchUpAgainAfterSlaveTcpClientClosed = true
        XGroup.tryCatchUpAgainAfterSlaveTcpClientClosed(replPair, contentBytes)
        then:
        XGroup.skipTryCatchUpAgainAfterSlaveTcpClientClosed

        cleanup:
        oneSlot.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test exists_reverse_index'() {
        given:
        def data4 = new byte[4][]
        def xGroup = new XGroup(null, data4, null)
        xGroup.replPair = ReplPairTest.mockAsMaster()

        and:
        ConfForGlobal.netListenAddresses = 'localhost:6379'

        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())

        def eventloopCurrent = Eventloop.builder()
                .withCurrentThread()
                .withIdleInterval(Duration.ofMillis(100))
                .build()

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        when:
        def contentBytes = new byte[1 + 4 + 4]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.put(XGroup.reverseIndexByteAsForMetaIndexWords)
        def r = xGroup.exists_reverse_index(slot, contentBytes)
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        r instanceof AsyncReply

        when:
        requestBuffer.position(0)
        requestBuffer.put(XGroup.reverseIndexByteAsForChunk)
        r = xGroup.exists_reverse_index(slot, contentBytes)
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        r instanceof AsyncReply

        when:
        def contentBytes2 = new byte[1 + 4 + 4 + XGroup.INDEX_EXISTS_WORDS_ONCE_READ_LENGTH]
        def requestBuffer2 = ByteBuffer.wrap(contentBytes2)
        requestBuffer2.put(XGroup.reverseIndexByteAsForMetaIndexWords)
        def r2 = xGroup.s_exists_reverse_index(slot, contentBytes2) as ReplReply
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        r2.isEmpty()

        when:
        // last batch for words
        def contentBytes22 = new byte[1 + 4 + 4 + 1024]
        def requestBuffer22 = ByteBuffer.wrap(contentBytes22)
        requestBuffer22.put(XGroup.reverseIndexByteAsForMetaIndexWords)
        requestBuffer22.put((byte) 0)
        requestBuffer22.putInt(0)
        def r22 = xGroup.s_exists_reverse_index(slot, contentBytes22) as ReplReply
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        r22.isEmpty()

        when:
        requestBuffer2.position(0)
        requestBuffer2.put(XGroup.reverseIndexByteAsForChunk)
        r2 = xGroup.s_exists_reverse_index(slot, contentBytes2) as ReplReply
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        r2.isEmpty()

        when:
        // last batch for chunk
        requestBuffer2.position(0)
        requestBuffer2.put(XGroup.reverseIndexByteAsForChunk)
        requestBuffer2.put((byte) 0)
        requestBuffer2.putInt(localPersist.indexHandlerPool.chunkMaxSegmentNumber - 1)
        r2 = xGroup.s_exists_reverse_index(slot, contentBytes2) as ReplReply
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        r2.isEmpty()

        when:
        // more than one index worker
        localPersist.indexHandlerPool.cleanUp()
        ConfForGlobal.indexWorkers = (byte) 2
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        // last batch for index worker 0 for words
        requestBuffer22.position(0)
        requestBuffer22.put(XGroup.reverseIndexByteAsForMetaIndexWords)
        requestBuffer22.put((byte) 0)
        requestBuffer22.putInt(0)
        r22 = xGroup.s_exists_reverse_index(slot, contentBytes22) as ReplReply
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        r22.isEmpty()

        when:
        // last batch for index worker 0 for chunk
        requestBuffer2.position(0)
        requestBuffer2.put(XGroup.reverseIndexByteAsForChunk)
        requestBuffer2.put((byte) 0)
        requestBuffer2.putInt(localPersist.indexHandlerPool.chunkMaxSegmentNumber - 1)
        r2 = xGroup.s_exists_reverse_index(slot, contentBytes2) as ReplReply
        eventloopCurrent.run()
        Thread.sleep(200)
        then:
        r2.isEmpty()

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
