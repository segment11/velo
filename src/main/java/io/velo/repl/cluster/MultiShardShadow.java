package io.velo.repl.cluster;

import java.util.ArrayList;

public class MultiShardShadow {
    private ArrayList<Shard> shards;

    public void setShards(ArrayList<Shard> shards) {
        this.shards = shards;
    }

    private Shard mySelfShard;

    public Shard getMySelfShard() {
        return mySelfShard;
    }

    public void setMySelfShard(Shard mySelfShard) {
        this.mySelfShard = mySelfShard;
    }

    public Shard getShardBySlot(int toClientSlot) {
        for (var shard : shards) {
            if (shard.contains(toClientSlot)) {
                return shard;
            }
        }

        return null;
    }
}
