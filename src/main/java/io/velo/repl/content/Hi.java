package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.Binlog;
import io.velo.repl.ReplContent;

public class Hi implements ReplContent {
    private final long slaveUuid;
    private final long masterUuid;
    private final Binlog.FileIndexAndOffset currentFo;
    private final Binlog.FileIndexAndOffset earliestFo;
    private final int currentSegmentIndex;

    public Hi(long slaveUuid, long masterUuid, Binlog.FileIndexAndOffset currentFo,
              Binlog.FileIndexAndOffset earliestFo, int currentSegmentIndex) {
        this.slaveUuid = slaveUuid;
        this.masterUuid = masterUuid;
        this.currentFo = currentFo;
        this.earliestFo = earliestFo;
        this.currentSegmentIndex = currentSegmentIndex;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeLong(slaveUuid);
        toBuf.writeLong(masterUuid);
        toBuf.writeInt(currentFo.fileIndex());
        toBuf.writeLong(currentFo.offset());
        toBuf.writeInt(earliestFo.fileIndex());
        toBuf.writeLong(earliestFo.offset());
        toBuf.writeInt(currentSegmentIndex);
    }

    @Override
    public int encodeLength() {
        return 8 + 8 + 4 + 8 + 4 + 8 + 4;
    }
}
