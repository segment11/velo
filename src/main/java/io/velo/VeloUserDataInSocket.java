package io.velo;

import io.velo.repl.ReplPair;

public class VeloUserDataInSocket {
    String authUser = null;

    boolean isResp3 = false;

    ReplPair replPairAsSlaveInTcpClient = null;

    public VeloUserDataInSocket() {
    }

    public VeloUserDataInSocket(ReplPair replPairAsSlaveInTcpClient) {
        this.replPairAsSlaveInTcpClient = replPairAsSlaveInTcpClient;
    }
}
