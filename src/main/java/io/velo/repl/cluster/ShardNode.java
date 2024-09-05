package io.velo.repl.cluster;

public class ShardNode {
    // for json
    public boolean isMaster() {
        return isMaster;
    }

    public void setMaster(boolean master) {
        isMaster = master;
    }

    public int getSlaveIndex() {
        return slaveIndex;
    }

    public void setSlaveIndex(int slaveIndex) {
        this.slaveIndex = slaveIndex;
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }

    public int getPort() {
        return port;
    }

    public void setPort(int port) {
        this.port = port;
    }

    public String getNodeId() {
        return nodeId;
    }

    public void setNodeId(String nodeId) {
        this.nodeId = nodeId;
    }

    boolean isMaster;
    int slaveIndex;
    String host;
    int port;
    String nodeId;


}
