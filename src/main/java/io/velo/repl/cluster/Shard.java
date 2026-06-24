package io.velo.repl.cluster;

import java.util.ArrayList;

/**
 * Represents a shard in the cluster containing nodes and slot ranges.
 */
public class Shard {
    /**
     * Returns the multi slot range owned by this shard.
     *
     * @return the multi slot range
     */
    // for json
    public MultiSlotRange getMultiSlotRange() {
        return multiSlotRange;
    }

    /**
     * Sets the multi slot range owned by this shard.
     *
     * @param multiSlotRange the multi slot range
     */
    public void setMultiSlotRange(MultiSlotRange multiSlotRange) {
        this.multiSlotRange = multiSlotRange;
    }

    /**
     * Returns the list of nodes in this shard.
     *
     * @return the list of nodes
     */
    public ArrayList<Node> getNodes() {
        return nodes;
    }

    /**
     * Sets the list of nodes in this shard.
     *
     * @param nodes the list of nodes
     */
    public void setNodes(ArrayList<Node> nodes) {
        this.nodes = nodes;
    }

    /**
     * Returns the slot currently being imported (migrated in) by this shard.
     *
     * @return the import migrating slot, or {@link #NO_MIGRATING_SLOT}
     */
    public int getImportMigratingSlot() {
        return importMigratingSlot;
    }

    /**
     * Sets the slot currently being imported (migrated in) by this shard.
     *
     * @param importMigratingSlot the import migrating slot
     */
    public void setImportMigratingSlot(int importMigratingSlot) {
        this.importMigratingSlot = importMigratingSlot;
    }

    /**
     * Returns the slot currently being exported (migrated out) by this shard.
     *
     * @return the export migrating slot, or {@link #NO_MIGRATING_SLOT}
     */
    public int getExportMigratingSlot() {
        return exportMigratingSlot;
    }

    /**
     * Sets the slot currently being exported (migrated out) by this shard.
     *
     * @param exportMigratingSlot the export migrating slot
     */
    public void setExportMigratingSlot(int exportMigratingSlot) {
        this.exportMigratingSlot = exportMigratingSlot;
    }

    /**
     * Returns the host that this shard is migrating to.
     *
     * @return the migrating target host, or null if not migrating
     */
    public String getMigratingToHost() {
        return migratingToHost;
    }

    /**
     * Sets the host that this shard is migrating to.
     *
     * @param migratingToHost the migrating target host
     */
    public void setMigratingToHost(String migratingToHost) {
        this.migratingToHost = migratingToHost;
    }

    /**
     * Returns the port that this shard is migrating to.
     *
     * @return the migrating target port
     */
    public int getMigratingToPort() {
        return migratingToPort;
    }

    /**
     * Sets the port that this shard is migrating to.
     *
     * @param migratingToPort the migrating target port
     */
    public void setMigratingToPort(int migratingToPort) {
        this.migratingToPort = migratingToPort;
    }

    private MultiSlotRange multiSlotRange = new MultiSlotRange();

    private ArrayList<Node> nodes = new ArrayList<>();

    private int importMigratingSlot = NO_MIGRATING_SLOT;
    private int exportMigratingSlot = NO_MIGRATING_SLOT;

    /**
     * Sentinel value indicating no slot is being migrated.
     */
    public static final int NO_MIGRATING_SLOT = -1;

    // only for from shard
    private String migratingToHost;

    private int migratingToPort;

    /**
     * Returns the master node of this shard.
     *
     * @return the master node, or null if none
     */
    public Node master() {
        for (var node : nodes) {
            if (node.isMaster) {
                return node;
            }
        }
        return null;
    }

    /**
     * Returns the slave node at the given index in this shard.
     *
     * @param slaveIndex the slave index
     * @return the slave node, or null if none
     */
    public Node slave(int slaveIndex) {
        for (var node : nodes) {
            if (!node.isMaster && node.slaveIndex == slaveIndex) {
                return node;
            }
        }
        return null;
    }

    /**
     * Returns the node in this shard that represents the local node itself.
     *
     * @return the local node, or null if none
     */
    public Node mySelfNode() {
        for (var node : nodes) {
            if (node.isMySelf) {
                return node;
            }
        }
        return null;
    }

    /**
     * Checks whether the given client slot is contained in this shard's slot ranges.
     *
     * @param toClientSlot the client slot
     * @return true if the slot is contained
     */
    public boolean contains(int toClientSlot) {
        return multiSlotRange.contains(toClientSlot);
    }

    /**
     * Returns the CLUSTER NODES formatted slot range lines for all nodes in this shard.
     *
     * @return the list of formatted node lines
     */
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
