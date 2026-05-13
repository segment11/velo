package io.velo.repl;

/**
 * An enumeration representing different types of replication messages in the Velo REPL system.
 * Each type has associated flags indicating whether it's after slave all catch up and whether
 * it's sent by a slave. It also has a unique byte code.
 */
public enum ReplType {
    /** Error message sent by master. */
    error(true, false, (byte) -100),

    /** Ping sent by slave to master. */
    ping(true, true, (byte) -1),

    /** Pong sent by master to slave. */
    pong(true, false, (byte) 1),

    /** Hello message sent by slave to master. */
    hello(true, true, (byte) 2),

    /** Hi message sent by master to slave. */
    hi(true, false, (byte) 3),

    /** Bye message sent by slave to master. */
    bye(true, true, (byte) 4),

    /** Bye message sent by master to slave. */
    byeBye(true, false, (byte) 5),

    /** Test message sent by slave to master. */
    test(true, true, (byte) 100),

    /** Request to master for fetching WAL entries (before catch-up). */
    exists_wal(false, true, (byte) 19),

    /** Request to master for fetching chunk segments (before catch-up). */
    exists_chunk_segments(false, true, (byte) 20),

    /** Request to master for fetching big strings (before catch-up). */
    exists_big_string(false, true, (byte) 21),

    /** Request to master for fetching short strings (before catch-up). */
    exists_short_string(false, true, (byte) 22),

    /** Request to master for fetching incremental big strings (after catch-up). */
    incremental_big_string(true, true, (byte) 41),

    /** Request to master for fetching dictionaries (before catch-up). */
    exists_dict(false, true, (byte) 25),

    /** Request to master for catch-up complete signal (before catch-up). */
    exists_all_done(false, true, (byte) 26),

    /** Request to master for incremental binlog (after catch-up). */
    catch_up(true, true, (byte) 27),

    /** Response containing WAL entries from master. */
    s_exists_wal(false, false, (byte) 29),

    /** Response containing chunk segments from master. */
    s_exists_chunk_segments(false, false, (byte) 30),

    /** Response containing big strings from master. */
    s_exists_big_string(false, false, (byte) 31),

    /** Response containing short strings from master. */
    s_exists_short_string(false, false, (byte) 32),

    /** Response containing incremental big strings from master. */
    s_incremental_big_string(true, false, (byte) 51),

    /** Response containing dictionaries from master. */
    s_exists_dict(false, false, (byte) 35),

    /** Response indicating all exists data sent from master. */
    s_exists_all_done(false, false, (byte) 36),

    /** Response containing incremental binlog from master (after catch-up). */
    s_catch_up(true, false, (byte) 37),
    ;

    /**
     * Flag indicating whether the message type is newly after slave all catch up.
     */
    public final boolean newly;

    /**
     * Flag indicating whether the message is sent by a slave.
     */
    public final boolean isSlaveSend;

    /**
     * Unique byte code for the message type.
     */
    public final byte code;

    /**
     * Constructs a new ReplType with the given properties.
     * newly       true if the message type is after slave all catch up.
     * isSlaveSend true if the message is sent by a slave.
     * code        the unique byte code for the message type.
     */
    ReplType(boolean newly, boolean isSlaveSend, byte code) {
        this.newly = newly;
        this.isSlaveSend = isSlaveSend;
        this.code = code;
    }

    /**
     * Retrieves the ReplType associated with the provided byte code.
     * code the byte code to look up.
     *
     * @return the corresponding ReplType, or null if no matching type is found.
     */
    public static ReplType fromCode(byte code) {
        for (var value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return null;
    }
}