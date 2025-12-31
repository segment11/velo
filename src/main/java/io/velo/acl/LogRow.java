package io.velo.acl;

import io.velo.reply.BulkReply;
import io.velo.reply.IntegerReply;
import io.velo.reply.Reply;

/**
 * For acl log command response
 */
public class LogRow {
    public int count;
    public String reason;
    public String context;
    public String object;
    public String username;
    public double ageSeconds;
    public String clientInfo;
    public long entryId;
    public long timestampCreated;
    public long timestampLastUpdated;

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
