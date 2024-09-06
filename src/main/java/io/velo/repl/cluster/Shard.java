package io.velo.repl.cluster;

import java.util.ArrayList;

public class Shard {
    // for json
    public MultiSlotRange getMultiSlotRange() {
        return multiSlotRange;
    }

    public void setMultiSlotRange(MultiSlotRange multiSlotRange) {
        this.multiSlotRange = multiSlotRange;
    }

    public ArrayList<Node> getNodes() {
        return nodes;
    }

    public void setNodes(ArrayList<Node> nodes) {
        this.nodes = nodes;
    }

    public int getMigratingSlot() {
        return migratingSlot;
    }

    public void setMigratingSlot(int migratingSlot) {
        this.migratingSlot = migratingSlot;
    }

    private MultiSlotRange multiSlotRange = new MultiSlotRange();

    private ArrayList<Node> nodes = new ArrayList<>();

    private int migratingSlot = -1;

    public Node master() {
        for (var node : nodes) {
            if (node.isMaster) {
                return node;
            }
        }
        return null;
    }

    public Node slave(int slaveIndex) {
        for (var node : nodes) {
            if (!node.isMaster && node.slaveIndex == slaveIndex) {
                return node;
            }
        }
        return null;
    }

    public boolean contains(int slot) {
        return multiSlotRange.contains(slot);
    }

    public ArrayList<String> clusterNodesSlotRangeList() {
        ArrayList<String> list = new ArrayList<>();
        if (this.multiSlotRange.getList().isEmpty()) {
            for (var node : nodes) {
                list.add(node.nodeInfoPrefix() + " - ");
            }
        } else {
            var allSlotRange = this.multiSlotRange.getList().stream().map(SlotRange::toString).reduce((a, b) -> a + " " + b).orElse("");
            for (var node : nodes) {
                list.add(node.nodeInfoPrefix() + " - " + allSlotRange);
            }
        }
        return list;
    }
}
