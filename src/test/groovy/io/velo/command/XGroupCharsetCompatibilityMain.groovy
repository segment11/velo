package io.velo.command

import io.velo.BaseCommand
import io.velo.persist.Consts
import io.velo.persist.LocalPersist
import io.velo.persist.LocalPersistTest
import io.velo.repl.Binlog
import io.velo.reply.ErrorReply

import java.io.IOException

class XGroupCharsetCompatibilityMain {
    private static void fail(String message) {
        System.err.println(message)
        System.exit(1)
    }

    static void main(String[] args) {
        final short slot = 0
        final short slotNumber = 2
        def localPersist = null

        try {
            def data9 = new byte[9][]
            data9[1] = 'slot'.bytes
            data9[2] = '0'.bytes
            data9[3] = 'x_catch_up'.bytes
            data9[4] = '10'.bytes

            def xGroup = new XGroup('x_repl', data9, null)
            xGroup.from(BaseCommand.mockAGroup())

            LocalPersistTest.prepareLocalPersist()
            localPersist = LocalPersist.instance
            localPersist.fixSlotThreadId(slot, Thread.currentThread().threadId())
            def oneSlot = localPersist.oneSlot(slot)
            oneSlot.createIfNotExistReplPairAsMaster(10L, 'localhost', 6380)

            def stubBinlog = new Binlog(slot, oneSlot.slotDir, oneSlot.dynConfig) {
                @Override
                FileIndexAndOffset currentFileIndexAndOffset() {
                    new Binlog.FileIndexAndOffset(0, 0L)
                }

                @Override
                byte[] readPrevRafOneSegment(int fileIndex, long offset) throws IOException {
                    throw new IOException('你好')
                }
            }
            oneSlot.setBinlog(stubBinlog)

            data9[5] = oneSlot.masterUuid.toString().bytes
            data9[6] = '0'.bytes
            data9[7] = '0'.bytes
            data9[8] = '0'.bytes
            xGroup.slotWithKeyHashListParsed = new XGroup(null, null, null).parseSlots('x_repl', data9, slotNumber)

            def reply = xGroup.handle()
            if (!(reply instanceof ErrorReply)) {
                fail("Expected ErrorReply but got ${reply?.class?.name}")
            }

            def message = (reply as ErrorReply).message
            if (message != 'Repl master handle error: read binlog file error=你好') {
                fail("Unexpected error message: ${message}")
            }
        } finally {
            localPersist?.cleanUp()
            Consts.persistDir.deleteDir()
        }
    }
}
