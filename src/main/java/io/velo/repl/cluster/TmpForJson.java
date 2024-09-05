package io.velo.repl.cluster;

import java.util.ArrayList;

public class TmpForJson {
    public ArrayList<SlotRange> getList() {
        return list;
    }

    public void setList(ArrayList<SlotRange> list) {
        this.list = list;
    }

    public ArrayList<ShardNode> getNodes() {
        return nodes;
    }

    public void setNodes(ArrayList<ShardNode> nodes) {
        this.nodes = nodes;
    }

    private ArrayList<SlotRange> list;
    private ArrayList<ShardNode> nodes;
}
