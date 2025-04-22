package io.velo.repl;

/**
 * An enumeration representing different types of replication messages in the Velo REPL system.
 * Each type has associated flags indicating whether it's after slave all catch up and whether
 * it's sent by a slave. It also has a unique byte code.
 */
public enum ReplType {
    /**
     * Represents an error message.
     * It's after slave all catch up.
     * It's sent by a master.
     * code byte value -1.
     */
    error(true, false, (byte) -1),

    /**
     * Represents a ping message.
     * It's after slave all catch up.
     * It's sent by a slave.
     * code byte value 0.
     */
    ping(true, true, (byte) 0),

    /**
     * Represents a pong message.
     * It's after slave all catch up.
     * It's sent by a master.
     * code byte value 1.
     */
    pong(true, false, (byte) 1),

    /**
     * Represents a hello message.
     * It's after slave all catch up.
     * It's sent by a slave.
     * code byte value 2.
     */
    hello(true, true, (byte) 2),

    /**
     * Represents a hi message.
     * It's after slave all catch up.
     * It's sent by a master.
     * code byte value 3.
     */
    hi(true, false, (byte) 3),

    /**
     * Represents a goodbye message.
     * It's after slave all catch up.
     * It's sent by a slave.
     * code byte value 4.
     */
    bye(true, true, (byte) 4),

    /**
     * Represents a goodbye message (variant).
     * It's after slave all catch up.
     * It's sent by a master.
     * code byte value 5.
     */
    byeBye(true, false, (byte) 5),

    /**
     * Represents a test message (variant).
     * It's after slave all catch up.
     * It's sent by a slave.
     * code byte value 100.
     */
    test(true, true, (byte) 100),

    /**
     * Request to master for fetching exists keys for analysis.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 18.
     */
    exists_keys_for_analysis(false, true, (byte) 18),

    /**
     * Request to master for fetching exists write-ahead log entries.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 19.
     */
    exists_wal(false, true, (byte) 19),

    /**
     * Request to master for fetching exists chunk segments.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 20.
     */
    exists_chunk_segments(false, true, (byte) 20),

    /**
     * Request to master for fetching exists key buckets.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 21.
     */
    exists_key_buckets(false, true, (byte) 21),

    /**
     * Request to master for fetching meta split number of key buckets.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 22.
     */
    meta_key_bucket_split_number(false, true, (byte) 22),

    /**
     * Request to master for fetching stats of key counts in buckets.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 23.
     */
    stat_key_count_in_buckets(false, true, (byte) 23),

    /**
     * Request to master for fetching exists big strings.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 24.
     */
    exists_big_string(false, true, (byte) 24),

    /**
     * Request to master for fetching incremental big strings.
     * It's after slave all catch up.
     * It's sent by a slave.
     * code byte value 44.
     */
    incremental_big_string(true, true, (byte) 44),

    /**
     * Request to master for fetching trained dictionaries.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 25.
     */
    exists_dict(false, true, (byte) 25),

    /**
     * Request to master for fetching exists all done.
     * It's before slave all catch up.
     * It's sent by a slave.
     * code byte value 26.
     */
    exists_all_done(false, true, (byte) 26),

    /**
     * Request to master for fetching incremental binlog.
     * It's after slave all catch up.
     * It's sent by a slave.
     * code byte value 27.
     */
    catch_up(true, true, (byte) 27),

    /**
     * Response to slave for fetching exists keys for analysis.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 28.
     */
    s_exists_keys_for_analysis(false, false, (byte) 28),

    /**
     * Response to slave for fetching exists write-ahead log entries.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 29.
     */
    s_exists_wal(false, false, (byte) 29),

    /**
     * Response to slave for fetching exists chunk segments.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 30.
     */
    s_exists_chunk_segments(false, false, (byte) 30),

    /**
     * Response to slave for fetching exists key buckets.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 31.
     */
    s_exists_key_buckets(false, false, (byte) 31),

    /**
     * Response to slave for fetching meta split number of key buckets.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 32.
     */
    s_meta_key_bucket_split_number(false, false, (byte) 32),

    /**
     * Response to slave for fetching stats of key counts in buckets.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 33.
     */
    s_stat_key_count_in_buckets(false, false, (byte) 33),

    /**
     * Response to slave for fetching exists big strings.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 34.
     */
    s_exists_big_string(false, false, (byte) 34),

    /**
     * Response to slave for fetching incremental big strings.
     * It's after slave all catch up.
     * It's sent by a master.
     * code byte value 54.
     */
    s_incremental_big_string(true, false, (byte) 54),

    /**
     * Response to slave for fetching exists trained dictionaries.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 35.
     */
    s_exists_dict(false, false, (byte) 35),

    /**
     * Response to slave for fetching exists all done.
     * It's before slave all catch up.
     * It's sent by a master.
     * code byte value 36.
     */
    s_exists_all_done(false, false, (byte) 36),

    /**
     * Response to slave for fetch incremental binlog after all catch up.
     * It's after slave all catch up.
     * It's sent by a master.
     * code byte value 37.
     */
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