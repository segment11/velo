package io.velo.repl;

import io.velo.repl.incremental.*;

import java.nio.ByteBuffer;

public interface BinlogContent {
    enum Type {
        // code need > 0
        wal((byte) 1), one_wal_group_persist((byte) 2), chunk_segment_flag_update((byte) 3),
        big_strings((byte) 10), dict((byte) 100), reverse_index_put_word((byte) 101),
        dyn_config(Byte.MAX_VALUE), flush(Byte.MIN_VALUE);

        private final byte code;

        Type(byte code) {
            this.code = code;
        }

        public byte code() {
            return code;
        }

        public static Type fromCode(byte code) {
            for (var type : values()) {
                if (type.code == code) {
                    return type;
                }
            }
            throw new IllegalArgumentException("Invalid binlog type code=" + code);
        }

        BinlogContent decodeFrom(ByteBuffer buffer) {
            return switch (this) {
                case wal -> XWalV.decodeFrom(buffer);
                case one_wal_group_persist -> XOneWalGroupPersist.decodeFrom(buffer);
                case chunk_segment_flag_update -> XChunkSegmentFlagUpdate.decodeFrom(buffer);
                case big_strings -> XBigStrings.decodeFrom(buffer);
                case dict -> XDict.decodeFrom(buffer);
                case reverse_index_put_word -> XReverseIndexPutWord.decodeFrom(buffer);
                case dyn_config -> XDynConfig.decodeFrom(buffer);
                case flush -> XFlush.decodeFrom(buffer);
            };
        }
    }

    Type type();

    int encodedLength();

    byte[] encodeWithType();

    @SlaveReplay
    void apply(short slot, ReplPair replPair);
}
