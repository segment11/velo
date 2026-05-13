package io.velo.repl;

import io.activej.promise.Promise;
import io.velo.repl.incremental.*;

import java.nio.ByteBuffer;

/**
 * Binary log content types for replication.
 */
public interface BinlogContent {
    /**
     * Binlog content type codes.
     */
    enum Type {
        /** WAL entry content type. */
        wal((byte) 1),
        /** Big strings content type. */
        big_strings((byte) 10),
        /** ACL update content type. */
        acl_update((byte) 20),
        /** Dictionary content type. */
        dict((byte) 100),
        /** Skip apply content type. */
        skip_apply((byte) 110),
        /** Update sequence content type. */
        update_seq((byte) 120),
        /** Dynamic config content type. */
        dyn_config((byte) 121),
        /** Flush content type. */
        flush(Byte.MAX_VALUE);

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

    /**
     * @return the type of this binlog content
     */
    Type type();

    /**
     * @return the encoded length in bytes
     */
    int encodedLength();

    /**
     * @return the encoded bytes including type byte
     */
    byte[] encodeWithType();

    /**
     * @param slot     the slot index
     * @param replPair the replication pair
     */
    @SlaveReplay
    void apply(short slot, ReplPair replPair);

    /**
     * @param slot     the slot index
     * @param replPair the replication pair
     * @return a promise that completes when the apply is done
     */
    @SlaveReplay
    default Promise<Void> applyAsync(short slot, ReplPair replPair) {
        apply(slot, replPair);
        return Promise.complete();
    }

    /**
     * @param slot the slot index
     * @return true if apply should be done asynchronously
     */
    default boolean isApplyAsync(short slot) {
        return false;
    }
}
