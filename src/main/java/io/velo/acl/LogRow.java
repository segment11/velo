package io.velo.acl;

import io.velo.reply.BulkReply;
import io.velo.reply.IntegerReply;
import io.velo.reply.Reply;

/**
 * ACL log entry for ACL log command response.
 */
public class LogRow {
    /** Log entry count. */
    public int count;
    /** ACL log reason. */
    public String reason;
    /** ACL log context. */
    public String context;
    /** ACL log object. */
    public String object;
    /** Username. */
    public String username;
    /** Age in seconds. */
    public double ageSeconds;
    /** Client information. */
    public String clientInfo;
    /** Entry ID. */
    public long entryId;
    /** Timestamp when entry was created. */
    public long timestampCreated;
    /** Timestamp when entry was last updated. */
    public long timestampLastUpdated;

    /**
     * Converts this log entry to Redis replies.
     * @return array of Redis replies representing the log entry
     */
    public Reply[] toReplies() {
        return new Reply[]{
                new BulkReply("count"),
                new IntegerReply(count),
                new BulkReply("reason"),
                new BulkReply(reason),
                new BulkReply("context"),
                new BulkReply(context),
                new BulkReply("object"),
                new BulkReply(object),
                new BulkReply("username"),
                new BulkReply(username),
                new BulkReply("age-seconds"),
                new BulkReply(ageSeconds),
                new BulkReply("client-info"),
                new BulkReply(clientInfo),
                new BulkReply("entry-id"),
                new IntegerReply(entryId),
                new BulkReply("timestamp-created"),
                new IntegerReply(timestampCreated),
                new BulkReply("timestamp-last-updated"),
                new IntegerReply(timestampLastUpdated)
        };
    }
}
