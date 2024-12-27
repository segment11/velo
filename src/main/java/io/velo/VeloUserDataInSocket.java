package io.velo;

import io.velo.repl.ReplPair;

public class VeloUserDataInSocket {
    String authUser = null;

    boolean isResp3 = false;

    boolean isConnectionReadonly = false;

    ReplPair replPairAsSlaveInTcpClient = null;

    byte[] lastScanTargetKeyBytes = null;

    long lastScanAssignCursor = 0;

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

    public VeloUserDataInSocket() {
    }

    public VeloUserDataInSocket(ReplPair replPairAsSlaveInTcpClient) {
        this.replPairAsSlaveInTcpClient = replPairAsSlaveInTcpClient;
    }
}
