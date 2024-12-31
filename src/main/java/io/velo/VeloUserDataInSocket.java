package io.velo;

import io.velo.repl.ReplPair;

public class VeloUserDataInSocket {
    String authUser = null;

    boolean isResp3 = false;

    boolean isConnectionReadonly = false;

    ReplPair replPairAsSlaveInTcpClient = null;

    private byte[] lastScanTargetKeyBytes = null;

    private long lastScanAssignCursor = 0;

    public byte[] getLastScanTargetKeyBytes() {
        return lastScanTargetKeyBytes;
    }

    public void setLastScanTargetKeyBytes(byte[] lastScanTargetKeyBytes) {
        this.lastScanTargetKeyBytes = lastScanTargetKeyBytes;
    }

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

    public VeloUserDataInSocket() {
    }

    public VeloUserDataInSocket(ReplPair replPairAsSlaveInTcpClient) {
        this.replPairAsSlaveInTcpClient = replPairAsSlaveInTcpClient;
    }
}
