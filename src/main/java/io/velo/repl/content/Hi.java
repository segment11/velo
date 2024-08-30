package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.Binlog;
import io.velo.repl.ReplContent;

public class Hi implements ReplContent {
    private final long slaveUuid;
    private final long masterUuid;
    private final Binlog.FileIndexAndOffset currentFileIndexAndOffset;
    private final Binlog.FileIndexAndOffset earliestFileIndexAndOffset;
    private final int currentSegmentIndex;

    public Hi(long slaveUuid, long masterUuid, Binlog.FileIndexAndOffset currentFileIndexAndOffset,
              Binlog.FileIndexAndOffset earliestFileIndexAndOffset, int currentSegmentIndex) {
        this.slaveUuid = slaveUuid;
        this.masterUuid = masterUuid;
        this.currentFileIndexAndOffset = currentFileIndexAndOffset;
        this.earliestFileIndexAndOffset = earliestFileIndexAndOffset;
        this.currentSegmentIndex = currentSegmentIndex;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeLong(slaveUuid);
        toBuf.writeLong(masterUuid);
        toBuf.writeInt(currentFileIndexAndOffset.fileIndex());
        toBuf.writeLong(currentFileIndexAndOffset.offset());
        toBuf.writeInt(earliestFileIndexAndOffset.fileIndex());
        toBuf.writeLong(earliestFileIndexAndOffset.offset());
        toBuf.writeInt(currentSegmentIndex);
    }

    @Override
    public int encodeLength() {
        return 8 + 8 + 4 + 8 + 4 + 8 + 4;
    }
}
