package io.velo.repl.cluster;

import java.util.ArrayList;

public class TmpForJson {

    public ArrayList<Shard> getShards() {
        return shards;
    }

    public void setShards(ArrayList<Shard> shards) {
        this.shards = shards;
    }

    public int getClusterMyEpoch() {
        return clusterMyEpoch;
    }

    public void setClusterMyEpoch(int clusterMyEpoch) {
        this.clusterMyEpoch = clusterMyEpoch;
    }

    public int getClusterCurrentEpoch() {
        return clusterCurrentEpoch;
    }

    public void setClusterCurrentEpoch(int clusterCurrentEpoch) {
        this.clusterCurrentEpoch = clusterCurrentEpoch;
    }

    private ArrayList<Shard> shards;

    private int clusterMyEpoch;

    private int clusterCurrentEpoch;
}
