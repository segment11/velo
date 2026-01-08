package io.velo.repl;

import io.velo.repl.incremental.*;

import java.nio.ByteBuffer;

public interface BinlogContent {
    enum Type {
        // code need > 0
        wal((byte) 1),
        big_strings((byte) 10), acl_update((byte) 20),
        dict((byte) 100), skip_apply((byte) 110), update_seq((byte) 120),
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
                case big_strings -> XBigStrings.decodeFrom(buffer);
                case acl_update -> XAclUpdate.decodeFrom(buffer);
                case dict -> XDict.decodeFrom(buffer);
                case skip_apply -> XSkipApply.decodeFrom(buffer);
                case update_seq -> XUpdateSeq.decodeFrom(buffer);
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

    default boolean isSkipWhenAllSlavesInCatchUpState() {
        return false;
    }
}
