package io.velo.repl.cluster;

import java.util.ArrayList;

/**
 * Temporary class for JSON serialization of cluster metadata.
 */
public class TmpForJson {

    /**
     * Returns the list of shards.
     *
     * @return the list of shards
     */
    public ArrayList<Shard> getShards() {
        return shards;
    }

    /**
     * Sets the list of shards.
     *
     * @param shards the list of shards
     */
    public void setShards(ArrayList<Shard> shards) {
        this.shards = shards;
    }

    /**
     * Returns this node's cluster epoch.
     *
     * @return the cluster my epoch
     */
    public int getClusterMyEpoch() {
        return clusterMyEpoch;
    }

    /**
     * Sets this node's cluster epoch.
     *
     * @param clusterMyEpoch the cluster my epoch
     */
    public void setClusterMyEpoch(int clusterMyEpoch) {
        this.clusterMyEpoch = clusterMyEpoch;
    }

    /**
     * Returns the current cluster epoch.
     *
     * @return the cluster current epoch
     */
    public int getClusterCurrentEpoch() {
        return clusterCurrentEpoch;
    }

    /**
     * Sets the current cluster epoch.
     *
     * @param clusterCurrentEpoch the cluster current epoch
     */
    public void setClusterCurrentEpoch(int clusterCurrentEpoch) {
        this.clusterCurrentEpoch = clusterCurrentEpoch;
    }

    private ArrayList<Shard> shards;

    private int clusterMyEpoch;

    private int clusterCurrentEpoch;
}
