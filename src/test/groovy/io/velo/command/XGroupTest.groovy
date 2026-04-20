package io.velo.command

import io.netty.buffer.Unpooled
import io.velo.*
import io.velo.mock.InMemoryGetSet
import io.velo.persist.*
import io.velo.repl.*
import io.velo.repl.Repl.ReplReply
import io.velo.repl.content.Hello
import io.velo.repl.content.Hi
import io.velo.repl.content.Ping
import io.velo.repl.content.Pong
import io.velo.repl.incremental.XWalV
import io.velo.reply.BulkReply
import io.velo.reply.ErrorReply
import io.velo.reply.NilReply
import spock.lang.Specification

import java.nio.ByteBuffer

class XGroupTest extends Specification {
    def _XXGroup = new XGroup(null, null, null)

    final short slot = 0
    final short slotNumber = 2

    private ReplRequest mockReplRequest(ReplPair replPair, ReplType replType, ReplContent content) {
        def reply = Repl.reply(slot, replPair, replType, content)
        mockReplRequest(reply)
    }

    private static ReplRequest mockReplRequest(ReplReply reply) {
        def nettyBuf = Unpooled.wrappedBuffer(reply.buffer().array())
        Repl.decode(nettyBuf)
    }

    private static String errorMessage(ReplReply reply) {
        Wal.keyString(mockReplRequest(reply).data)
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
        data4[2] = '10'.bytes
        sList = _XXGroup.parseSlots('x_repl', data4, slotNumber)
        then:
        sList.size() == 1
        // >= slot number, return %
        sList.first.slot() == (short) 0

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
        (reply as BulkReply).asString() == 'localhost:6380'

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
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test x catch up error reply uses decoded repl error payload'() {
        given:
        def data9 = new byte[9][]
        data9[1] = 'slot'.bytes
        data9[2] = '0'.bytes
        data9[3] = 'x_catch_up'.bytes
        data9[4] = '10'.bytes

        def xGroup = new XGroup('x_repl', data9, null)
        xGroup.from(BaseCommand.mockAGroup())

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.createIfNotExistReplPairAsMaster(10L, 'localhost', 6380)

        and:
        def stubBinlog = new Binlog(slot, oneSlot.slotDir, oneSlot.dynConfig) {
            @Override
            Binlog.FileIndexAndOffset currentFileIndexAndOffset() {
                new Binlog.FileIndexAndOffset(0, 0L)
            }

            @Override
            byte[] readPrevRafOneSegment(int fileIndex, long offset) throws IOException {
                throw new IOException('error')
            }
        }
        oneSlot.binlog = stubBinlog

        and:
        data9[5] = oneSlot.masterUuid.toString().bytes
        data9[6] = '0'.bytes
        data9[7] = '0'.bytes
        data9[8] = '0'.bytes
        xGroup.slotWithKeyHashListParsed = _XXGroup.parseSlots('x_repl', data9, slotNumber)

        when:
        def reply = xGroup.handle()

        then:
        reply instanceof ErrorReply
        (reply as ErrorReply).message == 'Repl master handle error: read binlog file error=error'

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test x catch up past eof returns error reply'() {
        given:
        def data9 = new byte[9][]
        data9[1] = 'slot'.bytes
        data9[2] = '0'.bytes
        data9[3] = 'x_catch_up'.bytes
        data9[4] = '10'.bytes

        def xGroup = new XGroup('x_repl', data9, null)
        xGroup.from(BaseCommand.mockAGroup())

        and:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)
        oneSlot.createIfNotExistReplPairAsMaster(10L, 'localhost', 6380)

        and:
        def oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        data9[5] = oneSlot.masterUuid.toString().bytes
        data9[6] = '0'.bytes
        data9[7] = (oneSegmentLength * 10).toString().bytes
        data9[8] = (oneSegmentLength * 10).toString().bytes
        xGroup.slotWithKeyHashListParsed = _XXGroup.parseSlots('x_repl', data9, slotNumber)

        when:
        def reply = xGroup.handle()

        then:
        reply instanceof ErrorReply

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test as master'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:6379'

        LocalPersistTest.prepareLocalPersist((byte) 1, slotNumber)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        when:
        // mock from slave repl request data
        final long slaveUuid = 1L
        def replPairAsSlave = ReplPairTest.mockAsSlave(0L, slaveUuid)
        def ping = new Ping('localhost:6380')
        def replRequest = mockReplRequest(replPairAsSlave, ReplType.ping, ping)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.pong)

        when:
        // handle ping again, already created repl pair as master when first received ping
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.pong)

        when:
        def malformedPing = new Ping('localhost:6380:extra')
        replRequest = mockReplRequest(replPairAsSlave, ReplType.ping, malformedPing)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        // hello
        when:
        def hello = new Hello(slaveUuid, '测试:6380')
        replRequest = mockReplRequest(replPairAsSlave, ReplType.hello, hello)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.hi)
        oneSlot.getReplPairAsMaster(slaveUuid) != null
        oneSlot.getReplPairAsMaster(slaveUuid).remoteReplProperties != null
        oneSlot.getReplPairAsMaster(slaveUuid).port == 6380
        oneSlot.dynConfig.binlogOn

        when:
        def shortHelloBytes = new byte[8 + 4]
        def shortHelloBuffer = ByteBuffer.wrap(shortHelloBytes)
        shortHelloBuffer.putLong(slaveUuid)
        shortHelloBuffer.putInt(0)
        replRequest.data = shortHelloBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        replRequest = mockReplRequest(replPairAsSlave, ReplType.hello, hello)
        def malformedHelloBytes = new byte[replRequest.data.length + 1]
        System.arraycopy(replRequest.data, 0, malformedHelloBytes, 0, replRequest.data.length)
        malformedHelloBytes[malformedHelloBytes.length - 1] = (byte) 1
        replRequest.data = malformedHelloBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def malformedHelloAddress = new Hello(slaveUuid, '测试:6380:extra')
        replRequest = mockReplRequest(replPairAsSlave, ReplType.hello, malformedHelloAddress)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        replRequest = mockReplRequest(replPairAsSlave, ReplType.hello, hello)
        ByteBuffer.wrap(replRequest.data).putInt(8 + 4 + '测试:6380'.getBytes('UTF-8').length + 2 + 4, 0)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        replRequest = mockReplRequest(replPairAsSlave, ReplType.hello, hello)
        replRequest.data[replRequest.data.length - 1] = (byte) 2
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        replRequest = mockReplRequest(replPairAsSlave, ReplType.hello, hello)
        x.replPair = null
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.hi)

        when:
        // slot remote >= slot number
        replRequest.slot = ConfForGlobal.slotNumber
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        // bye
        when:
        replRequest = mockReplRequest(replPairAsSlave, ReplType.bye, new Ping('localhost:6379:extra'))
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        replRequest = mockReplRequest(replPairAsSlave, ReplType.bye, ping)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.byeBye)

        when:
        // remove repl pair
        oneSlot.doTask(0)
        x.replPair = null
        def replPairAsMaster = oneSlot.firstReplPairAsMaster
        oneSlot.replPairAsMasterList
        replPairAsMaster.bye()
        r = x.handleRepl(replRequest)
        then:
        // empty
        r.isEmpty()

        when:
        replPairAsMaster.sendBye = false
        // master receive hello from slave, then create repl pair again
        replRequest = mockReplRequest(replPairAsSlave, ReplType.hello, hello)
        x.handleRepl(replRequest)
        replRequest.slaveUuid = slaveUuid
        replRequest.type = ReplType.exists_chunk_segments

        // begin segment index
        // chunk segment bytes exists
        oneSlot.metaChunkSegmentFlagSeq.setSegmentMergeFlag(0, Chunk.Flag.init.flagByte(), 1, 0)
        def contentBytes = new byte[4]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_exists_chunk_segments)
        // begin segment index, segment count, segment batch count, segment length, no data
        r.buffer().limit() == Repl.HEADER_LENGTH + 16

        when:
        contentBytes = new byte[3]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[5]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        // response exists wal
        when:
        contentBytes = new byte[4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(ConfForSlot.global.confChunk.maxSegmentNumber())
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        replRequest.type = ReplType.exists_wal
        contentBytes = new byte[4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_exists_wal)

        when:
        contentBytes = new byte[3]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[5]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def walGroupNumber = Wal.calcWalGroupNumber()
        contentBytes = new byte[4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(walGroupNumber)
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[4]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(-1)
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        // incremental_big_string
        when:
        replRequest.type = ReplType.incremental_big_string
        def bigStringUuid = 1L
        def bigStringKey = 'big-string'
        def sBigString = BaseCommand.slot(bigStringKey, ConfForGlobal.slotNumber)
        oneSlot.bigStringFiles.writeBigStringBytes(bigStringUuid, sBigString.bucketIndex(), sBigString.keyHash(), new byte[1024])
        contentBytes = new byte[8 + 4 + bigStringKey.length()]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(bigStringUuid)
        requestBuffer.putInt(bigStringKey.length())
        requestBuffer.put(bigStringKey.getBytes())
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_incremental_big_string)
        r.buffer().limit() == Repl.HEADER_LENGTH + 8 + 4 + bigStringKey.length() + 1024

        when:
        def malformedIncrementalBigStringBytes = new byte[contentBytes.length + 1]
        System.arraycopy(contentBytes, 0, malformedIncrementalBigStringBytes, 0, contentBytes.length)
        malformedIncrementalBigStringBytes[malformedIncrementalBigStringBytes.length - 1] = (byte) 1
        replRequest.data = malformedIncrementalBigStringBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        // exists_big_string
        when:
        replRequest.type = ReplType.exists_big_string
        contentBytes = new byte[4 + 8 * 2]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // bucket index
        requestBuffer.putInt(sBigString.bucketIndex())
        // master has one big string
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_exists_big_string)

        when:
        oneSlot.bigStringFiles.deleteBigStringFileIfExist(bigStringUuid, sBigString.bucketIndex(), sBigString.keyHash())
        // master has no big string
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_exists_big_string)

        when:
        requestBuffer.position(0)
        requestBuffer.putInt(ConfForSlot.global.confBucket.bucketsPerSlot)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[4 + 8 * 2 + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(sBigString.bucketIndex())
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        // exists_short_string
        when:
        replRequest.type = ReplType.exists_short_string
        contentBytes = new byte[4]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // wal group index
        requestBuffer.putInt(0)
        def r2 = x.handleRepl(replRequest)
        then:
        r2 instanceof Repl.ReplReplyFromBytes

        when:
        contentBytes = new byte[3]
        replRequest.data = contentBytes
        def r3 = x.handleRepl(replRequest) as ReplReply
        then:
        r3.isReplType(ReplType.error)

        when:
        contentBytes = new byte[5]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        r3 = x.handleRepl(replRequest) as ReplReply
        then:
        r3.isReplType(ReplType.error)

        // exists_dict
        when:
        replRequest.type = ReplType.exists_dict
        // byte[1] means next step
        contentBytes = new byte[2]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[4 * 2 + 1]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(1)
        requestBuffer.putInt(2)
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[4 * 2]
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(1)
        requestBuffer.putInt(2)
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_exists_dict)

        // exists_all_done
        when:
        replRequest.type = ReplType.exists_all_done
        contentBytes = [0] as byte[]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_exists_all_done)

        when:
        contentBytes = new byte[0]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = [1] as byte[]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = [0, 0] as byte[]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        // catch_up
        when:
        replRequest.type = ReplType.catch_up
        contentBytes = new byte[8 + 4 + 8 + 8]
        replRequest.data = contentBytes
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
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        requestBuffer.position(0)
        requestBuffer.putLong(oneSlot.masterUuid)
        r = x.handleRepl(replRequest)
        then:
        // empty content return
        r.isReplType(ReplType.s_catch_up)

        when:
        contentBytes = new byte[8 + 4 + 8 + 8 + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(oneSlot.masterUuid)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putLong(0)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[8 + 4 + 8 + 7]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(oneSlot.masterUuid)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.put((byte) 0)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[8 + 4 + 8 + 8]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(oneSlot.masterUuid)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putLong(0)
        oneSlot.readonly = true
        r = x.handleRepl(replRequest)
        then:
        // empty content return
        r.isReplType(ReplType.s_catch_up)

        when:
        def vList = Mock.prepareValueList(10)
        for (v in vList) {
            def xWalV = new XWalV(v)
            oneSlot.appendBinlog(xWalV)
        }
        // read segment bytes < one segment length
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8 + 4 + 8)
        requestBuffer.putLong(106)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8 + 4 + 8)
        requestBuffer.putLong(oneSlot.binlog.currentFileIndexAndOffset().offset())
        r = x.handleRepl(replRequest)
        then:
        // empty content return
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(8)
        // file index not match master binlog current file index, read fail
        requestBuffer.putInt(1)
        requestBuffer.putLong(0)
        requestBuffer.putLong(oneSlot.binlog.currentFileIndexAndOffset().offset())
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // mock as binlog file index match
        def binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        def binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength
        (binlogOneFileMaxLength / binlogOneSegmentLength).intValue().times {
            oneSlot.binlog.moveToNextSegment(true)
        }
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.s_catch_up)

        when:
        requestBuffer.position(0)
        requestBuffer.putLong(oneSlot.masterUuid)
        requestBuffer.putInt(0)
        // not margin
        requestBuffer.putLong(1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test as slave'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:6380'

        LocalPersistTest.prepareLocalPersist((byte) 1, slotNumber)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        when:
        // mock from master repl response data
        final long masterUuid = 10L
        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid
        def pong = new Pong('localhost:6379')
        def replRequest = mockReplRequest(replPairAsMaster, ReplType.pong, pong)
        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        oneSlot.createReplPairAsSlave('localhost', 6379)
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        replRequest = mockReplRequest(replPairAsMaster, ReplType.pong, new Pong('localhost:6379:extra'))
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // error
        replRequest = mockReplRequest(Repl.error(slot, replPairAsMaster, 'error'))
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        // hi
        when:
        def metaChunkSegmentIndex = oneSlot.metaChunkSegmentIndex
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false, 0, 0L)
        def hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.exists_dict)

        when:
        // first fetch dict, send local exist dict seq
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.persistDir)
        dictMap.putDict('key:', new Dict(new byte[10]))
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.exists_dict)

        when:
        hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        ByteBuffer.wrap(replRequest.data).putInt(46 + 4, 0)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // already fetch all exists data, just catch up binlog
        // from beginning
        hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // last updated offset not margined
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 106L)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // last updated offset margined
        def binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, binlogOneSegmentLength)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.catch_up)

        when:
        // not match slave uuid
        hi = new Hi(replPairAsMaster.slaveUuid + 1, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def shortHiBytes = new byte[64 - 1]
        replRequest.data = shortHiBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        def malformedHiBytes = new byte[replRequest.data.length + 1]
        System.arraycopy(replRequest.data, 0, malformedHiBytes, 0, replRequest.data.length)
        malformedHiBytes[malformedHiBytes.length - 1] = (byte) 1
        replRequest.data = malformedHiBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        // ok
        when:
        hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        replRequest.data[replRequest.data.length - 1] = (byte) 2
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        replRequest = mockReplRequest(Repl.test(slot, replPairAsMaster, 'test'))
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        // byeBye
        when:
        replRequest = mockReplRequest(replPairAsMaster, ReplType.byeBye, pong)
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        replRequest.slaveUuid = replPairAsMaster.slaveUuid
        replRequest.slot = slot
        replRequest.type = ReplType.s_exists_chunk_segments

        def contentBytes = new byte[16 + 4096]
        replRequest.data = contentBytes
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        // begin segment index
        requestBuffer.putInt(0)
        // segment count
        requestBuffer.putInt(1)
        // segment batch count
        requestBuffer.putInt(ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl)
        // segment length
        requestBuffer.putInt(ConfForSlot.global.confChunk.segmentLength)

        def list = Mock.prepareValueList(800)
        ArrayList<PersistValueMeta> returnPvmList = []
        def snowFlake = new SnowFlake(1, 1)
        def segmentBatch2 = new SegmentBatch2(snowFlake)
        def splitResult = segmentBatch2.split(list, returnPvmList)
        requestBuffer.put(splitResult.getFirst().segmentBytes())
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.exists_chunk_segments)

        when:
        def malformedChunkContentBytes = new byte[17]
        replRequest.data = malformedChunkContentBytes
        def malformedChunkRequestBuffer = ByteBuffer.wrap(malformedChunkContentBytes)
        malformedChunkRequestBuffer.putInt(0)
        malformedChunkRequestBuffer.putInt(0)
        malformedChunkRequestBuffer.putInt(ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl)
        malformedChunkRequestBuffer.putInt(ConfForSlot.global.confChunk.segmentLength)
        malformedChunkRequestBuffer.put((byte) 1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def zeroBatchChunkContentBytes = new byte[16]
        replRequest.data = zeroBatchChunkContentBytes
        def zeroBatchChunkRequestBuffer = ByteBuffer.wrap(zeroBatchChunkContentBytes)
        zeroBatchChunkRequestBuffer.putInt(0)
        zeroBatchChunkRequestBuffer.putInt(0)
        zeroBatchChunkRequestBuffer.putInt(0)
        zeroBatchChunkRequestBuffer.putInt(ConfForSlot.global.confChunk.segmentLength)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def invalidCountChunkContentBytes = new byte[16 + ConfForSlot.global.confChunk.segmentLength * 2]
        replRequest.data = invalidCountChunkContentBytes
        def invalidCountChunkRequestBuffer = ByteBuffer.wrap(invalidCountChunkContentBytes)
        invalidCountChunkRequestBuffer.putInt(0)
        invalidCountChunkRequestBuffer.putInt(2)
        invalidCountChunkRequestBuffer.putInt(1)
        invalidCountChunkRequestBuffer.putInt(ConfForSlot.global.confChunk.segmentLength)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def invalidRangeChunkContentBytes = new byte[16]
        replRequest.data = invalidRangeChunkContentBytes
        def invalidRangeChunkRequestBuffer = ByteBuffer.wrap(invalidRangeChunkContentBytes)
        invalidRangeChunkRequestBuffer.putInt(ConfForSlot.global.confChunk.maxSegmentNumber() - 1)
        invalidRangeChunkRequestBuffer.putInt(0)
        invalidRangeChunkRequestBuffer.putInt(2)
        invalidRangeChunkRequestBuffer.putInt(ConfForSlot.global.confChunk.segmentLength)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // segment compression
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(1)
        requestBuffer.putInt(ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl)
        requestBuffer.putInt(ConfForSlot.global.confChunk.segmentLength)
        var segmentBatch = new SegmentBatch(snowFlake)
        def splitAndTightResult = segmentBatch.split(list, returnPvmList)
        requestBuffer.position(16).put(splitAndTightResult.getFirst().segmentBytes())
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.exists_chunk_segments)

        when:
        def badSegmentLength = ConfForSlot.global.confChunk.segmentLength + 128
        def badContentBytes = new byte[16 + badSegmentLength]
        replRequest.data = badContentBytes
        def badRequestBuffer = ByteBuffer.wrap(badContentBytes)
        badRequestBuffer.putInt(0)
        badRequestBuffer.putInt(1)
        badRequestBuffer.putInt(ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl)
        badRequestBuffer.putInt(badSegmentLength)
        badRequestBuffer.put(splitResult.getFirst().segmentBytes())
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        replRequest.data = contentBytes
        // last batch
        requestBuffer.position(0)
        requestBuffer.putInt(ConfForSlot.global.confChunk.maxSegmentNumber() - ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl)
        requestBuffer.position(16)
        // segment no data
        requestBuffer.put(new byte[4096])
        r = x.handleRepl(replRequest)
        then:
        // next step
        r.isReplType(ReplType.exists_wal)

        when:
        requestBuffer.position(0)
        // next batch will delay run
        requestBuffer.putInt(ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl * 1023)
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        // exists wal
        when:
        replRequest.type = ReplType.s_exists_wal
        contentBytes = new byte[8 + 2 * Wal.ONE_GROUP_BUFFER_SIZE]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // wal group index
        requestBuffer.putInt(0)
        requestBuffer.putInt(Wal.ONE_GROUP_BUFFER_SIZE)
        def vList2 = Mock.prepareValueList(20, 0)
        var vEncodedBytes = vList2[-1].encode(true)
        requestBuffer.put(vEncodedBytes)
        r = x.handleRepl(replRequest)
        then:
        // next batch
        r.isReplType(ReplType.exists_wal)

        when:
        contentBytes = new byte[9]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(0)
        requestBuffer.put((byte) 1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def invalidWalSlot = x.findKeyMustInSlot(slot, 'wal-invalid-')
        def invalidWalKey = invalidWalSlot.rawKey()
        def invalidWalCv = new CompressedValue()
        invalidWalCv.seq = 12L
        invalidWalCv.keyHash = invalidWalSlot.keyHash()
        invalidWalCv.compressedData = new byte[10]
        def invalidWalV = new Wal.V(12L, invalidWalSlot.bucketIndex(), invalidWalSlot.keyHash(),
                CompressedValue.NO_EXPIRE, CompressedValue.NULL_DICT_SEQ,
                invalidWalKey, invalidWalCv.encode(), false)
        contentBytes = new byte[8 + 2 * Wal.ONE_GROUP_BUFFER_SIZE]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(-1)
        requestBuffer.putInt(Wal.ONE_GROUP_BUFFER_SIZE)
        requestBuffer.put(invalidWalV.encode(true))
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        oneSlot.get(invalidWalKey, invalidWalSlot.bucketIndex(), invalidWalSlot.keyHash()) == null

        when:
        // skip, no log
        contentBytes = new byte[4]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(1023)
        r = x.handleRepl(replRequest)
        then:
        // delay fetch next batch
        r.isEmpty()

        when:
        // skip, trigger log
        contentBytes = new byte[4]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(1024)
        r = x.handleRepl(replRequest)
        then:
        // next batch
        r.isReplType(ReplType.exists_wal)

        when:
        // last batch
        def walGroupNumber = Wal.calcWalGroupNumber()
        requestBuffer.position(0)
        requestBuffer.putInt(walGroupNumber - 1)
        r = x.handleRepl(replRequest)
        then:
        // next step
        r.isReplType(ReplType.exists_all_done)

        // s_incremental_big_string
        when:
        def bigStringUuid = 1L
        def bigStringKey = 'big-string'
        def sBigString = BaseCommand.slot(bigStringKey, ConfForGlobal.slotNumber)
        replRequest.type = ReplType.s_incremental_big_string
        contentBytes = new byte[8 + 4 + bigStringKey.length() + 1024]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putLong(bigStringUuid)
        requestBuffer.putInt(bigStringKey.length())
        requestBuffer.put(bigStringKey.getBytes())
        requestBuffer.put(new byte[1024])
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        replRequest.type = ReplType.s_exists_big_string
        contentBytes = new byte[4 + 4 + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // bucket index
        requestBuffer.putInt(0)
        // big string count
        requestBuffer.putInt(0)
        // is sent all once flag
        requestBuffer.put((byte) 1)
        r = x.handleRepl(replRequest)
        then:
        // next bucket index
        r.isReplType(ReplType.exists_big_string)

        when:
        contentBytes = new byte[4 + 4 + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(-1)
        requestBuffer.putInt(0)
        requestBuffer.put((byte) 1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[4 + 4 + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(0)
        requestBuffer.put((byte) 2)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        requestBuffer.position(0)
        requestBuffer.putInt(1)
        requestBuffer.putInt(0)
        // is sent all once flag
        requestBuffer.put((byte) 0)
        r = x.handleRepl(replRequest)
        then:
        // continue current bucket index
        r.isReplType(ReplType.exists_big_string)

        when:
        // last bucket index
        requestBuffer.position(0)
        requestBuffer.putInt(ConfForSlot.global.confBucket.bucketsPerSlot - 1)
        requestBuffer.putInt(0)
        // is sent all once flag
        requestBuffer.put((byte) 1)
        r = x.handleRepl(replRequest)
        then:
        // next step
        r.isReplType(ReplType.exists_short_string)

        when:
        // mock two big string fetched
        contentBytes = new byte[4 + 4 + 1 + (8 + 8 + 4 + 1024) * 2]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(2)
        requestBuffer.put((byte) 1)
        // big string uuid
        requestBuffer.putLong(1L)
        // key hash
        requestBuffer.putLong(1L)
        requestBuffer.putInt(1024)
        requestBuffer.position(requestBuffer.position() + 1024)
        requestBuffer.putLong(2L)
        requestBuffer.putLong(2L)
        requestBuffer.putInt(1024)
        r = x.handleRepl(replRequest)
        then:
        // next bucket index
        r.isReplType(ReplType.exists_big_string)

        when:
        // is sent all false
        requestBuffer.put(4 + 4, (byte) 0)
        r = x.handleRepl(replRequest)
        then:
        // next batch
        r.isReplType(ReplType.exists_big_string)

        when:
        // last bucket index
        requestBuffer.position(0)
        requestBuffer.putInt(ConfForSlot.global.confBucket.bucketsPerSlot - 1)
        // is sent all true
        requestBuffer.put(4 + 4, (byte) 1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.exists_short_string)

        when:
        // malformed negative count should not advance repl state
        contentBytes = new byte[4 + 4 + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(-1)
        requestBuffer.put((byte) 1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // malformed short payload should not read header
        contentBytes = new byte[4 + 4]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(0)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // malformed trailing bytes should not be ignored
        contentBytes = new byte[4 + 4 + 1 + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(0)
        requestBuffer.put((byte) 1)
        requestBuffer.put((byte) 1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // malformed entry header should not read partial entry metadata
        contentBytes = new byte[4 + 4 + 1 + 8]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(1)
        requestBuffer.put((byte) 1)
        requestBuffer.putLong(1L)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // malformed entry length should not overrun payload
        contentBytes = new byte[4 + 4 + 1 + 8 + 8 + 4 + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(0)
        requestBuffer.putInt(1)
        requestBuffer.put((byte) 1)
        requestBuffer.putLong(1L)
        requestBuffer.putLong(2L)
        requestBuffer.putInt(2)
        requestBuffer.put((byte) 1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def inMemoryGetSet = new InMemoryGetSet()
        x.byPassGetSet = inMemoryGetSet
        def s = x.findKeyMustInSlot(slot, "exists_big_string_uuids_bucket_index_" + 0)
        x.remove(s)
        r = x.fetchExistsBigString(slot, 0)
        then:
        // empty content return
        r.isReplType(ReplType.exists_big_string)

        // s_exists_short_string
        when:
        replRequest.type = ReplType.s_exists_short_string
        // wal group index + short value encoded
        contentBytes = new byte[4]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.exists_short_string)

        when:
        contentBytes = new byte[4]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(-1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def invalidShortStringSlot = x.findKeyMustInSlot(slot, 'short-string-invalid-')
        def invalidShortStringKey = invalidShortStringSlot.rawKey()
        def invalidShortStringCv = new CompressedValue()
        invalidShortStringCv.seq = 11L
        invalidShortStringCv.keyHash = invalidShortStringSlot.keyHash()
        invalidShortStringCv.compressedData = new byte[10]
        def invalidShortStringCvEncoded = invalidShortStringCv.encode()
        def invalidShortStringEncodedLength = 8 + 8 + 8 + 4 + invalidShortStringKey.length() + 4 + invalidShortStringCvEncoded.length
        contentBytes = new byte[4 + 4 + invalidShortStringEncodedLength]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(-1)
        requestBuffer.putInt(invalidShortStringEncodedLength)
        requestBuffer.putLong(invalidShortStringCv.seq)
        requestBuffer.putLong(invalidShortStringCv.keyHash)
        requestBuffer.putLong(0L)
        requestBuffer.putInt(invalidShortStringKey.length())
        requestBuffer.put(invalidShortStringKey.bytes)
        requestBuffer.putInt(invalidShortStringCvEncoded.length)
        requestBuffer.put(invalidShortStringCvEncoded)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        oneSlot.get(invalidShortStringKey, invalidShortStringSlot.bucketIndex(), invalidShortStringSlot.keyHash()) == null

        when:
        var keyTest = 'key:000000000001'
        def cv = new CompressedValue()
        cv.seq = 1L
        cv.keyHash = KeyHash.hash(keyTest.bytes)
        cv.compressedData = new byte[10]
        def cvEncoded = cv.encode()
        contentBytes = new byte[4 + 4 + 48 + cvEncoded.length]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        // wal group index
        requestBuffer.putInt(0)
        requestBuffer.putInt(48 + cvEncoded.length)
        requestBuffer.putLong(cv.seq)
        requestBuffer.putLong(cv.keyHash)
        requestBuffer.putLong(0L)
        requestBuffer.putInt(16)
        requestBuffer.put(keyTest.bytes)
        requestBuffer.putInt(cvEncoded.length)
        requestBuffer.put(cvEncoded)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.exists_short_string)

        when:
        // last batch
        requestBuffer.position(0)
        requestBuffer.putInt(walGroupNumber - 1)
        r = x.handleRepl(replRequest)
        then:
        // next step
        r.isReplType(ReplType.exists_chunk_segments)

        // s_exists_dict
        when:
        replRequest.type = ReplType.s_exists_dict
        // dict count int
        contentBytes = new byte[4]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        // next step
        r.isReplType(ReplType.exists_big_string)

        when:
        def dict1 = new Dict(new byte[10])
        def dict2 = new Dict(new byte[20])
        def encoded1 = dict1.encode('k1')
        def encoded2 = dict2.encode('k2')
        contentBytes = new byte[4 + 4 + encoded1.length + 4 + encoded2.length]
        replRequest.data = contentBytes;
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.putInt(2)
        requestBuffer.putInt(encoded1.length)
        requestBuffer.put(encoded1)
        requestBuffer.putInt(encoded2.length)
        requestBuffer.put(encoded2)
        r = x.handleRepl(replRequest)
        then:
        // next step
        r.isReplType(ReplType.exists_big_string)
        dictMap.getDict('k1') != null
        dictMap.getDict('k2') != null

        // s_exists_all_done
        when:
        replRequest.type = ReplType.s_exists_all_done
        contentBytes = [0] as byte[]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        // next step
        r.isReplType(ReplType.catch_up)
        oneSlot.metaChunkSegmentIndex.isExistsDataAllFetched()

        when:
        contentBytes = new byte[0]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = [1] as byte[]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = [0, 0] as byte[]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        // s_catch_up
        when:
        // clear slave catch up binlog file index and offset
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        replRequest.type = ReplType.s_catch_up
        // mock 10 wal values in binlog
        int n = 0
        def vList = Mock.prepareValueList(10)
        for (v in vList) {
            n += new XWalV(v).encodedLength()
        }
        contentBytes = new byte[1 + 4 + 8 + 4 + 8 + 4 + n]
        replRequest.data = contentBytes
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
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        contentBytes = new byte[1 + 4 + 8 + 4 + 8 + 4 + n + 1]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.put((byte) 0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(n)
        requestBuffer.putInt(n)
        for (v in vList) {
            def encoded = new XWalV(v).encodeWithType()
            requestBuffer.put(encoded)
        }
        requestBuffer.put((byte) 1)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        contentBytes = new byte[1 + 4 + 8 + 4 + 8 + 4]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.put((byte) 0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        def oldCatchUpOffsetMinDiff = ConfForSlot.global.confRepl.catchUpOffsetMinDiff
        ConfForSlot.global.confRepl.catchUpOffsetMinDiff = n
        oneSlot.setCanRead(false)
        contentBytes = new byte[1 + 4 + 8 + 4 + 8 + 4 + n]
        replRequest.data = contentBytes
        requestBuffer = ByteBuffer.wrap(contentBytes)
        requestBuffer.put((byte) 0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(n + 1L)
        requestBuffer.putInt(n)
        for (v in vList) {
            def encoded = new XWalV(v).encodeWithType()
            requestBuffer.put(encoded)
        }
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()
        oneSlot.isCanRead()

        when:
        contentBytes[0] = (byte) 2
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        ConfForSlot.global.confRepl.catchUpOffsetMinDiff = oldCatchUpOffsetMinDiff
        contentBytes = new byte[1 + 4 + 8 + 4 + 8 + 4 + binlogOneSegmentLength]
        replRequest.data = contentBytes
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
        r = x.handleRepl(replRequest)
        then:
        // next batch
        r.isReplType(ReplType.catch_up)
        oneSlot.metaChunkSegmentIndex.masterBinlogFileIndexAndOffset.fileIndex() == 0
        oneSlot.metaChunkSegmentIndex.masterBinlogFileIndexAndOffset.offset() == binlogOneSegmentLength

        when:
        requestBuffer.position(1 + 4)
        requestBuffer.putLong(binlogOneSegmentLength)
        r = x.handleRepl(replRequest)
        then:
        // next batch
        r.isReplType(ReplType.catch_up)

        when:
        requestBuffer.position(1 + 4)
        requestBuffer.putLong(0)
        requestBuffer.putInt(0)
        requestBuffer.putLong(binlogOneSegmentLength - 1)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, mockSkipN)
        r = x.handleRepl(replRequest)
        then:
        // delay to fetch next batch
        r.isEmpty()

        when:
        requestBuffer.position(1 + 4)
        requestBuffer.putLong(0)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        r = x.handleRepl(replRequest)
        then:
        // next batch
        r.isReplType(ReplType.catch_up)

        when:
        // master readonly
        contentBytes[0] = (byte) 1
        r = x.handleRepl(replRequest)
        then:
        // next batch
        r.isReplType(ReplType.catch_up)

        when:
        def binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength
        requestBuffer.position(1 + 4)
        requestBuffer.putLong(binlogOneFileMaxLength - binlogOneSegmentLength)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(binlogOneSegmentLength)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        r = x.handleRepl(replRequest)
        then:
        // begin with new binlog file next batch, delay
        r.isEmpty()

        when:
        requestBuffer.position(1 + 4)
        requestBuffer.putLong(0)
        requestBuffer.putInt(1)
        requestBuffer.putLong(binlogOneSegmentLength)
        requestBuffer.putInt(1)
        requestBuffer.put((byte) 0)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // not slot 0 catch up, wait slot 0
        localPersist.asSlaveFirstSlotFetchedExistsAllDone = false
        replRequest.slot = 1
        // slot 1
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        replRequest.slot = 0
        // only readonly flag from master, mean no more binlog bytes
        contentBytes = new byte[13]
        replRequest.data = contentBytes
        contentBytes[0] = (byte) 1
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()
        oneSlot.onlyOneReplPairAsSlave.allCaughtUp && oneSlot.onlyOneReplPairAsSlave.masterReadonly

        when:
        // only readonly flag from master, mean no more binlog bytes
        contentBytes[0] = (byte) 0
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()
        oneSlot.onlyOneReplPairAsSlave.allCaughtUp && !oneSlot.onlyOneReplPairAsSlave.masterReadonly

        when:
        contentBytes[0] = (byte) 2
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // pong trigger slave do catch up again
        contentBytes = new byte[2]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        contentBytes = [1, 0] as byte[]
        replRequest.data = contentBytes
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        when:
        // pong, trigger catch up
        replRequest = mockReplRequest(replPairAsMaster, ReplType.pong, pong)
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, false, 0, 0L)
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        // pong, trigger catch up
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        oneSlot.onlyOneReplPairAsSlave.lastGetCatchUpResponseMillis = 0
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        // need no trigger catch up
        oneSlot.onlyOneReplPairAsSlave.lastGetCatchUpResponseMillis = System.currentTimeMillis() - 1000
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        // trigger catch up
        oneSlot.onlyOneReplPairAsSlave.lastGetCatchUpResponseMillis = System.currentTimeMillis() - 10000
        r = x.handleRepl(replRequest)
        then:
        r.isEmpty()

        when:
        replRequest = mockReplRequest(replPairAsMaster, ReplType.byeBye, new Pong('localhost:6379:extra'))
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        cleanup:
        ConfForSlot.global.confRepl.catchUpOffsetMinDiff = 1024 * 1024
        localPersist.cleanUp()
        dictMap.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed exists short string payload'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def contentBytes = ByteBuffer.allocate(5)
                .putInt(0)
                .put((byte) 1)
                .array()
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_short_string, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('remote repl properties missing before short string')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects too short exists short string payload'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def contentBytes = new byte[3]
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_short_string, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects exists short string wal group out of range'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def slaveUuid = 11L
        oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, 'localhost', 6380)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        def negativeBytes = ByteBuffer.allocate(4)
                .putInt(-1)
                .array()
        ReplReply r = x.handleRepl(new ReplRequest(slaveUuid, slot, ReplType.exists_short_string, negativeBytes, negativeBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('exists short string wal group index invalid')

        when:
        def tooLargeBytes = ByteBuffer.allocate(4)
                .putInt(Wal.calcWalGroupNumber())
                .array()
        r = x.handleRepl(new ReplRequest(slaveUuid, slot, ReplType.exists_short_string, tooLargeBytes, tooLargeBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('exists short string wal group index invalid')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed exists dict payload'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def contentBytes = ByteBuffer.allocate(4)
                .putInt(-1)
                .array()
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_dict, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects too short exists dict payload'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def contentBytes = new byte[3]
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_dict, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed exists dict payload before saving dict'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        def dictMap = DictMap.instance
        dictMap.initDictMap(Consts.persistDir)
        dictMap.clearAll()

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def dict = new Dict(new byte[10])
        def encoded = dict.encode('key:')
        def contentBytes = ByteBuffer.allocate(4 + 4 + encoded.length + 1)
                .putInt(1)
                .putInt(encoded.length)
                .put(encoded)
                .put((byte) 7)
                .array()
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_dict, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('dict payload has trailing bytes')
        dictMap.getDict('key:') == null
        dictMap.dictSize() == 0

        cleanup:
        dictMap.clearAll()
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects exists wal before remote repl properties ready'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def contentBytes = ByteBuffer.allocate(4)
                .putInt(0)
                .array()
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_wal, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('remote repl properties missing before wal')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects chunk segments before remote repl properties ready'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def contentBytes = ByteBuffer.allocate(16)
                .putInt(0)
                .putInt(0)
                .putInt(ConfForSlot.global.confChunk.onceReadSegmentCountWhenRepl)
                .putInt(ConfForSlot.global.confChunk.segmentLength)
                .array()
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_chunk_segments, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('remote repl properties missing before chunk segments')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects exists big string before remote repl properties ready'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def contentBytes = ByteBuffer.allocate(9)
                .putInt(0)
                .putInt(0)
                .put((byte) 1)
                .array()
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_big_string, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('remote repl properties missing before big string')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed incremental big string payload'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def masterUuid = 33L
        oneSlot.createReplPairAsSlave('localhost', 7379)
        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        def tooShortBytes = new byte[8 + 4 - 1]
        ReplReply r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_incremental_big_string, tooShortBytes, tooShortBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('incremental big string payload too short')

        when:
        def invalidKeyLengthBytes = ByteBuffer.allocate(8 + 4)
                .putLong(1L)
                .putInt(1)
                .array()
        r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_incremental_big_string, invalidKeyLengthBytes, invalidKeyLengthBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('incremental big string key length invalid')

        when:
        def negativeKeyLengthBytes = ByteBuffer.allocate(8 + 4)
                .putLong(1L)
                .putInt(-1)
                .array()
        r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_incremental_big_string, negativeKeyLengthBytes, negativeKeyLengthBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('incremental big string key length invalid')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed catch up positions'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def masterUuid = 44L
        oneSlot.createReplPairAsSlave('localhost', 7379)
        oneSlot.metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)

        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        and:
        def v = Mock.prepareValueList(1).first()
        def segmentBytes = new XWalV(v).encodeWithType()

        when:
        def negativeFetchedFileIndexBytes = ByteBuffer.allocate(1 + 4 + 8 + 4 + 8 + 4 + segmentBytes.length)
                .put((byte) 0)
                .putInt(-1)
                .putLong(0L)
                .putInt(0)
                .putLong(segmentBytes.length)
                .putInt(segmentBytes.length)
                .put(segmentBytes)
                .array()
        ReplReply r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_catch_up, negativeFetchedFileIndexBytes, negativeFetchedFileIndexBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('catch up fetched file index invalid')

        when:
        def negativeFetchedOffsetBytes = ByteBuffer.allocate(1 + 4 + 8 + 4 + 8 + 4 + segmentBytes.length)
                .put((byte) 0)
                .putInt(0)
                .putLong(-1L)
                .putInt(0)
                .putLong(segmentBytes.length)
                .putInt(segmentBytes.length)
                .put(segmentBytes)
                .array()
        r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_catch_up, negativeFetchedOffsetBytes, negativeFetchedOffsetBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('catch up fetched offset invalid')

        when:
        def negativeCurrentFileIndexBytes = ByteBuffer.allocate(13)
                .put((byte) 0)
                .putInt(-1)
                .putLong(0L)
                .array()
        r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_catch_up, negativeCurrentFileIndexBytes, negativeCurrentFileIndexBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('catch up current file index invalid')

        when:
        def negativeCurrentOffsetBytes = ByteBuffer.allocate(13)
                .put((byte) 0)
                .putInt(0)
                .putLong(-1L)
                .array()
        r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_catch_up, negativeCurrentOffsetBytes, negativeCurrentOffsetBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('catch up current offset invalid')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed master catch up positions'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def slaveUuid = 55L
        oneSlot.createIfNotExistReplPairAsMaster(slaveUuid, 'localhost', 6380)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        def negativeFileIndexBytes = ByteBuffer.allocate(8 + 4 + 8 + 8)
                .putLong(oneSlot.masterUuid)
                .putInt(-1)
                .putLong(0L)
                .putLong(0L)
                .array()
        ReplReply r = x.handleRepl(new ReplRequest(slaveUuid, slot, ReplType.catch_up,
                negativeFileIndexBytes, negativeFileIndexBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('catch up need fetch file index invalid')

        when:
        def negativeNeedFetchOffsetBytes = ByteBuffer.allocate(8 + 4 + 8 + 8)
                .putLong(oneSlot.masterUuid)
                .putInt(0)
                .putLong(-ConfForSlot.global.confRepl.binlogOneSegmentLength)
                .putLong(0L)
                .array()
        r = x.handleRepl(new ReplRequest(slaveUuid, slot, ReplType.catch_up,
                negativeNeedFetchOffsetBytes, negativeNeedFetchOffsetBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('catch up need fetch offset invalid')

        when:
        def negativeLastUpdatedOffsetBytes = ByteBuffer.allocate(8 + 4 + 8 + 8)
                .putLong(oneSlot.masterUuid)
                .putInt(0)
                .putLong(0L)
                .putLong(-1L)
                .array()
        r = x.handleRepl(new ReplRequest(slaveUuid, slot, ReplType.catch_up,
                negativeLastUpdatedOffsetBytes, negativeLastUpdatedOffsetBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('catch up last updated offset invalid')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed hi positions'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def masterUuid = 66L
        oneSlot.createReplPairAsSlave('localhost', 7379)
        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        def hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        def replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        ByteBuffer.wrap(replRequest.data).putInt(16, -1)
        ReplReply r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('hi current file index invalid')

        when:
        hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        ByteBuffer.wrap(replRequest.data).putLong(20, -1L)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('hi current offset invalid')

        when:
        hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        ByteBuffer.wrap(replRequest.data).putInt(28, -1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('hi earliest file index invalid')

        when:
        hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        ByteBuffer.wrap(replRequest.data).putLong(32, -1L)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('hi earliest offset invalid')

        when:
        hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 1L),
                new Binlog.FileIndexAndOffset(0, 0L), 0)
        replRequest = mockReplRequest(replPairAsMaster, ReplType.hi, hi)
        ByteBuffer.wrap(replRequest.data).putInt(40, -1)
        r = x.handleRepl(replRequest)
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('hi current segment index invalid')

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects unaligned slave catch up offset before state change'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def masterUuid = 99L
        oneSlot.createReplPairAsSlave('localhost', 7379)
        oneSlot.metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 0L)
        def replPairAsSlave = oneSlot.onlyOneReplPairAsSlave

        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        and:
        def v = Mock.prepareValueList(1).first()
        def segmentBytes = new XWalV(v).encodeWithType()
        def unalignedOffset = 1L

        when:
        def contentBytes = ByteBuffer.allocate(1 + 4 + 8 + 4 + 8 + 4 + segmentBytes.length)
                .put((byte) 0)
                .putInt(0)
                .putLong(unalignedOffset)
                .putInt(0)
                .putLong(unalignedOffset + segmentBytes.length)
                .putInt(segmentBytes.length)
                .put(segmentBytes)
                .array()
        ReplReply r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_catch_up, contentBytes, contentBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('catch up fetched offset invalid')
        !replPairAsSlave.allCaughtUp
        replPairAsSlave.masterBinlogCurrentFileIndexAndOffset == null
        replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset == null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed catch up binlog before state change'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def masterUuid = 100L
        oneSlot.createReplPairAsSlave('localhost', 7379)
        // Set lastUpdatedOffset to 5, so skipBytesN will be 5 when fetchedOffset is 0
        oneSlot.metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 0, 5L)
        def replPairAsSlave = oneSlot.onlyOneReplPairAsSlave

        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        and:
        // Invalid segment bytes with type code=2, but readSegmentLength is only 3 (< skipBytesN=5)
        def invalidSegmentBytes = [(byte) 2, (byte) 0, (byte) 0] as byte[]

        when:
        // Construct payload: readonlyFlag(1) + fetchedFileIndex(0) + fetchedOffset(0) + 
        // masterCurrentFileIndex(0) + masterCurrentOffset(3) + readSegmentLength(3) + segmentBytes
        def contentBytes = ByteBuffer.allocate(1 + 4 + 8 + 4 + 8 + 4 + invalidSegmentBytes.length)
                .put((byte) 1)
                .putInt(0)
                .putLong(0L)
                .putInt(0)
                .putLong(invalidSegmentBytes.length)
                .putInt(invalidSegmentBytes.length)
                .put(invalidSegmentBytes)
                .array()
        ReplReply r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_catch_up, contentBytes, contentBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('skip bytes n=5')
        errorMessage(r).contains('is greater than read segment length=3')
        !replPairAsSlave.masterReadonly
        !replPairAsSlave.allCaughtUp
        replPairAsSlave.masterBinlogCurrentFileIndexAndOffset == null
        replPairAsSlave.slaveLastCatchUpBinlogFileIndexAndOffset == null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test hello hi reply uses binlog offset after skip apply append'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:6379'

        LocalPersistTest.prepareLocalPersist((byte) 1, slotNumber)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        final long slaveUuid = 1L
        def replPairAsSlave = ReplPairTest.mockAsSlave(0L, slaveUuid)
        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())

        when:
        def ping = new Ping('localhost:6380')
        x.handleRepl(mockReplRequest(replPairAsSlave, ReplType.ping, ping))
        ReplReply r = x.handleRepl(mockReplRequest(replPairAsSlave, ReplType.hello, new Hello(slaveUuid, 'localhost:6380')))
        def hiRequest = mockReplRequest(r)
        def hiBuffer = ByteBuffer.wrap(hiRequest.data)
        hiBuffer.getLong()
        hiBuffer.getLong()
        hiBuffer.getInt()
        def currentOffset = hiBuffer.getLong()
        def currentFo = oneSlot.binlog.currentFileIndexAndOffset()

        then:
        r.isReplType(ReplType.hi)
        currentOffset == currentFo.offset()
        currentOffset > 0

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test hi skips exists fetch when last updated file is newer than earliest file'() {
        given:
        ConfForGlobal.netListenAddresses = 'localhost:6380'

        LocalPersistTest.prepareLocalPersist((byte) 1, slotNumber)
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        final long masterUuid = 10L
        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        and:
        def metaChunkSegmentIndex = oneSlot.metaChunkSegmentIndex
        metaChunkSegmentIndex.setMasterBinlogFileIndexAndOffset(masterUuid, true, 1, 0L)

        when:
        def hi = new Hi(replPairAsMaster.slaveUuid, masterUuid,
                new Binlog.FileIndexAndOffset(1, 200L),
                new Binlog.FileIndexAndOffset(0, 100L), 0)
        ReplReply r = x.handleRepl(mockReplRequest(replPairAsMaster, ReplType.hi, hi))

        then:
        r.isReplType(ReplType.catch_up)

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects malformed big string payload before persisting entry'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        def masterUuid = 111L
        oneSlot.createReplPairAsSlave('localhost', 7379)
        oneSlot.onlyOneReplPairAsSlave.setRemoteReplProperties(ConfForSlot.global.generateReplProperties())
        def replPairAsMaster = ReplPairTest.mockAsMaster(masterUuid)
        replPairAsMaster.slaveUuid = oneSlot.masterUuid

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        and:
        def bigStringSlot = x.findKeyMustInSlot(slot, 'exists-big-string')
        def bigStringUuid = 123L
        def bigStringBytes = new byte[32]
        def sSet = x.findKeyMustInSlot(slot, 'exists_big_string_uuids_bucket_index_' + bigStringSlot.bucketIndex())

        when:
        def contentBytes = ByteBuffer.allocate(4 + 4 + 1 + 8 + 8 + 4 + bigStringBytes.length + 1)
                .putInt(bigStringSlot.bucketIndex())
                .putInt(1)
                .put((byte) 1)
                .putLong(bigStringUuid)
                .putLong(bigStringSlot.keyHash())
                .putInt(bigStringBytes.length)
                .put(bigStringBytes)
                .put((byte) 7)
                .array()
        ReplReply r = x.handleRepl(new ReplRequest(replPairAsMaster.slaveUuid, slot,
                ReplType.s_exists_big_string, contentBytes, contentBytes.length))
        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('big string payload has trailing bytes')
        oneSlot.bigStringFiles.getBigStringFileIdList(bigStringSlot.bucketIndex()).find { it.uuid() == bigStringUuid } == null
        SGroup.getRedisSet(sSet, x) == null

        cleanup:
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }

    def 'test handle repl rejects invalid current bucket done flag'() {
        given:
        LocalPersistTest.prepareLocalPersist()
        def localPersist = LocalPersist.instance
        localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
        def oneSlot = localPersist.oneSlot(slot)

        and:
        ConfForGlobal.indexWorkers = (byte) 1
        localPersist.startIndexHandlerPool()
        Thread.sleep(1000)

        and:
        oneSlot.createReplPairAsSlave('localhost', 6379)

        def contentBytes = ByteBuffer.allocate(4 + 4 + 1)
                .putInt(0)
                .putInt(0)
                .put((byte) 2)
                .array()
        def replRequest = new ReplRequest(oneSlot.masterUuid, slot, ReplType.s_exists_big_string, contentBytes, contentBytes.length)

        def x = new XGroup(null, null, null)
        x.from(BaseCommand.mockAGroup())
        x.replPair = null

        when:
        ReplReply r = x.handleRepl(replRequest)

        then:
        r.isReplType(ReplType.error)
        errorMessage(r).contains('big string current-bucket-done flag invalid')

        cleanup:
        localPersist.cleanUp()
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
        def contentBytes = new byte[1 + 4 + 4 + 4 + 8 + 4 + 8 + 4 + n]
        def requestBuffer = ByteBuffer.wrap(contentBytes)
        // is readonly flag
        requestBuffer.put((byte) 0)
        requestBuffer.putInt(0)
        requestBuffer.putInt(0)
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
        localPersist.cleanUp()
        Consts.persistDir.deleteDir()
    }
}
