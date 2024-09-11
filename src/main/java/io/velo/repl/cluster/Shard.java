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

    public int getImportMigratingSlot() {
        return importMigratingSlot;
    }

    public void setImportMigratingSlot(int importMigratingSlot) {
        this.importMigratingSlot = importMigratingSlot;
    }

    public int getExportMigratingSlot() {
        return exportMigratingSlot;
    }

    public void setExportMigratingSlot(int exportMigratingSlot) {
        this.exportMigratingSlot = exportMigratingSlot;
    }

    public String getMigratingToHost() {
        return migratingToHost;
    }

    public void setMigratingToHost(String migratingToHost) {
        this.migratingToHost = migratingToHost;
    }

    public int getMigratingToPort() {
        return migratingToPort;
    }

    public void setMigratingToPort(int migratingToPort) {
        this.migratingToPort = migratingToPort;
    }

    private MultiSlotRange multiSlotRange = new MultiSlotRange();

    private ArrayList<Node> nodes = new ArrayList<>();

    private int importMigratingSlot = NO_MIGRATING_SLOT;
    private int exportMigratingSlot = NO_MIGRATING_SLOT;

    public static final int NO_MIGRATING_SLOT = -1;

    // only for from shard
    private String migratingToHost;

    private int migratingToPort;

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

    public Node mySelfNode() {
        for (var node : nodes) {
            if (node.isMySelf) {
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

    @Override
    public String toString() {
        return "Shard{" +
                "multiSlotRange=" + multiSlotRange +
                ", nodes=" + nodes +
                ", importMigratingSlot=" + importMigratingSlot +
                ", exportMigratingSlot=" + exportMigratingSlot +
                ", migratingToHost='" + migratingToHost + '\'' +
                ", migratingToPort=" + migratingToPort +
                '}';
    }
}
