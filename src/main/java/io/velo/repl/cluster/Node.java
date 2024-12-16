package io.velo.repl.cluster;

import io.velo.Utils;

public class Node {
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

    public boolean isMySelf() {
        return isMySelf;
    }

    public void setMySelf(boolean mySelf) {
        isMySelf = mySelf;
    }

    public String getFollowNodeId() {
        return followNodeId;
    }

    public void setFollowNodeId(String followNodeId) {
        this.followNodeId = followNodeId;
    }

    public String getNodeIdFix() {
        return nodeIdFix;
    }

    public void setNodeIdFix(String nodeIdFix) {
        this.nodeIdFix = nodeIdFix;
    }

    boolean isMaster;
    int slaveIndex;
    String host;
    int port;
    boolean isMySelf;
    private String followNodeId;
    private String nodeIdFix;

    private static final int NODE_ID_LENGTH = 40;
    private static final String NODE_ID_PREFIX = "velo_node_";

    public String nodeId() {
        if (nodeIdFix != null) {
            return nodeIdFix;
        }

        String nodeIdShort;
        if (host.contains(".")) {
            nodeIdShort = NODE_ID_PREFIX + host.replace(".", "_") + "_" + port + "_";
        } else {
            nodeIdShort = NODE_ID_PREFIX + host + "_" + port + "_";
        }

        // padding
        return Utils.rightPad(nodeIdShort, "0", NODE_ID_LENGTH);
    }

    public String nodeInfoPrefix() {
        return nodeId() + " " + host + ":" + port + " " +
                (isMySelf ? "myself," : "") +
                (isMaster ? "master" : "slave") +
                (isMaster ? "" : " " + followNodeId);
    }

    @Override
    public String toString() {
        return "Node{" +
                "isMaster=" + isMaster +
                ", slaveIndex=" + slaveIndex +
                ", host='" + host + '\'' +
                ", port=" + port +
                ", isMySelf=" + isMySelf +
                ", followNodeId='" + followNodeId + '\'' +
                ", nodeId='" + nodeId() + '\'' +
                '}';
    }
}
