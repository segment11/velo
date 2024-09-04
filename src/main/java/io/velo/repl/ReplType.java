package io.velo.repl;

public enum ReplType {
    error(true, false, (byte) -1),
    ping(true, true, (byte) 0),
    pong(true, false, (byte) 1),
    hello(true, true, (byte) 2),
    hi(true, false, (byte) 3),
    bye(true, true, (byte) 4),
    byeBye(true, false, (byte) 5),
    test(true, true, (byte) 100),

    exists_reverse_index(false, true, (byte) 49),
    s_exists_reverse_index(false, true, (byte) 50),

    exists_wal(false, true, (byte) 19),
    exists_chunk_segments(false, true, (byte) 20),
    exists_key_buckets(false, true, (byte) 21),
    meta_key_bucket_split_number(false, true, (byte) 22),
    stat_key_count_in_buckets(false, true, (byte) 23),
    exists_big_string(false, true, (byte) 24),
    incremental_big_string(true, true, (byte) 44),
    exists_dict(false, true, (byte) 25),
    exists_all_done(false, true, (byte) 26),
    catch_up(true, true, (byte) 27),

    s_exists_wal(false, false, (byte) 29),
    s_exists_chunk_segments(false, false, (byte) 30),
    s_exists_key_buckets(false, false, (byte) 31),
    s_meta_key_bucket_split_number(false, false, (byte) 32),
    s_stat_key_count_in_buckets(false, false, (byte) 33),
    s_exists_big_string(false, false, (byte) 34),
    s_incremental_big_string(true, false, (byte) 54),
    s_exists_dict(false, false, (byte) 35),
    s_exists_all_done(false, false, (byte) 36),
    s_catch_up(true, false, (byte) 37),
    ;

    public final boolean newly;
    public final boolean isSlaveSend;
    public final byte code;

    ReplType(boolean newly, boolean isSlaveSend, byte code) {
        this.newly = newly;
        this.isSlaveSend = isSlaveSend;
        this.code = code;
    }

    public static ReplType fromCode(byte code) {
        for (var value : values()) {
            if (value.code == code) {
                return value;
            }
        }
        return null;
    }
}
