package io.velo;

import io.velo.repl.ReplPair;

public class VeloUserDataInSocket {
    String authUser = null;

    boolean isResp3 = false;

    boolean isConnectionReadonly = false;

    public enum ReplyMode {
        on, off, skip;

        public static ReplyMode from(String str) {
            return switch (str) {
                case "off" -> off;
                case "skip" -> skip;
                default -> on;
            };
        }
    }

    ReplyMode replyMode = ReplyMode.on;

    public void setReplyMode(ReplyMode replyMode) {
        this.replyMode = replyMode;
    }

    private long lastScanAssignCursor = 0;

    public long getLastScanAssignCursor() {
        return lastScanAssignCursor;
    }

    public void setLastScanAssignCursor(long lastScanAssignCursor) {
        this.lastScanAssignCursor = lastScanAssignCursor;
    }

    private String clientName;

    public String getClientName() {
        return clientName;
    }

    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    ReplPair replPairAsSlaveInTcpClient = null;

    public VeloUserDataInSocket() {
    }

    public VeloUserDataInSocket(ReplPair replPairAsSlaveInTcpClient) {
        this.replPairAsSlaveInTcpClient = replPairAsSlaveInTcpClient;
    }
}
