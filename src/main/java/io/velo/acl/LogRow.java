package io.velo.acl;

import io.velo.reply.BulkReply;
import io.velo.reply.IntegerReply;
import io.velo.reply.Reply;

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
                new BulkReply("count".getBytes()),
                new IntegerReply(count),
                new BulkReply("reason".getBytes()),
                new BulkReply(reason.getBytes()),
                new BulkReply("context".getBytes()),
                new BulkReply(context.getBytes()),
                new BulkReply("object".getBytes()),
                new BulkReply(object.getBytes()),
                new BulkReply("username".getBytes()),
                new BulkReply(username.getBytes()),
                new BulkReply("age-seconds".getBytes()),
                new BulkReply(String.valueOf(ageSeconds).getBytes()),
                new BulkReply("client-info".getBytes()),
                new BulkReply(clientInfo.getBytes()),
                new BulkReply("entry-id".getBytes()),
                new IntegerReply(entryId),
                new BulkReply("timestamp-created".getBytes()),
                new IntegerReply(timestampCreated),
                new BulkReply("timestamp-last-updated".getBytes()),
                new IntegerReply(timestampLastUpdated)
        };
    }
}
