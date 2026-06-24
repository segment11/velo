package io.velo.repl.cluster;

import io.velo.Utils;
import io.velo.repl.support.JedisCallback;
import io.velo.repl.support.JedisPoolHolder;

/**
 * Represents a node in the cluster.
 */
public class Node {
    /**
     * Returns whether this node is a master.
     *
     * @return true if this node is a master
     */
    // for json
    public boolean isMaster() {
        return isMaster;
    }

    /**
     * Sets whether this node is a master.
     *
     * @param master true if this node is a master
     */
    public void setMaster(boolean master) {
        isMaster = master;
    }

    /**
     * Returns the slave index of this node.
     *
     * @return the slave index
     */
    public int getSlaveIndex() {
        return slaveIndex;
    }

    /**
     * Sets the slave index of this node.
     *
     * @param slaveIndex the slave index
     */
    public void setSlaveIndex(int slaveIndex) {
        this.slaveIndex = slaveIndex;
    }

    /**
     * Returns the host of this node.
     *
     * @return the host
     */
    public String getHost() {
        return host;
    }

    /**
     * Sets the host of this node.
     *
     * @param host the host
     */
    public void setHost(String host) {
        this.host = host;
    }

    /**
     * Returns the port of this node.
     *
     * @return the port
     */
    public int getPort() {
        return port;
    }

    /**
     * Sets the port of this node.
     *
     * @param port the port
     */
    public void setPort(int port) {
        this.port = port;
    }

    /**
     * Returns whether this node represents the local node itself.
     *
     * @return true if this node is the local node
     */
    public boolean isMySelf() {
        return isMySelf;
    }

    /**
     * Sets whether this node represents the local node itself.
     *
     * @param mySelf true if this node is the local node
     */
    public void setMySelf(boolean mySelf) {
        isMySelf = mySelf;
    }

    /**
     * Returns the node id that this slave follows (its master's node id).
     *
     * @return the follow node id
     */
    public String getFollowNodeId() {
        return followNodeId;
    }

    /**
     * Sets the node id that this slave follows (its master's node id).
     *
     * @param followNodeId the follow node id
     */
    public void setFollowNodeId(String followNodeId) {
        this.followNodeId = followNodeId;
    }

    /**
     * Returns the fixed node id, if set explicitly.
     *
     * @return the fixed node id, or null if not set
     */
    public String getNodeIdFix() {
        return nodeIdFix;
    }

    /**
     * Sets the fixed node id.
     *
     * @param nodeIdFix the fixed node id
     */
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

    /**
     * Returns the node id, using the fixed one if set, otherwise generating one from the host and port.
     *
     * @return the node id
     */
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

    /**
     * Returns the node info prefix used in the Redis CLUSTER NODES output format.
     *
     * @return the node info prefix string
     */
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

    /**
     * Execute redis command to this node
     *
     * @param callback the Jedis callback
     * @param <R>      the result
     * @return the result
     */
    public <R> R exe(JedisCallback<R> callback) {
        var jedisPoolHolder = JedisPoolHolder.getInstance();
        var jedisPool = jedisPoolHolder.createIfNotCached(host, port);
        return JedisPoolHolder.exe(jedisPool, callback);
    }
}
