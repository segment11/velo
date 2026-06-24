package io.velo.repl.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.velo.ConfForGlobal;
import io.velo.persist.LocalPersist;
import io.velo.repl.ReplPair;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

/**
 * Manages multiple shards in the cluster.
 */
public class MultiShard {
    /**
     * The total number of client-facing slots, matching Redis cluster's slot count.
     */
    public static final short TO_CLIENT_SLOT_NUMBER = 16384;

    /**
     * Converts a client slot to the inner (sharded) slot index.
     *
     * @param toClientSlot the client slot
     * @return the inner slot index
     */
    public static short asInnerSlotByToClientSlot(int toClientSlot) {
        return (short) (toClientSlot / (TO_CLIENT_SLOT_NUMBER / ConfForGlobal.slotNumber));
    }

    /**
     * Checks whether the given client slot should be skipped (not handled by this node).
     *
     * @param toClientSlot the client slot
     * @return true if the client slot should be skipped
     */
    public static boolean isToClientSlotSkip(int toClientSlot) {
        return toClientSlot % (TO_CLIENT_SLOT_NUMBER / ConfForGlobal.slotNumber) != 0;
    }

    /*
refer to redis cluster nodes.conf
7ce84f442ac179271dac46ce4f3f2feb73d3a0e4 :0@0 myself,master - 0 0 0 connected
vars currentEpoch 0 lastVoteEpoch 0
     */
    static final String metaFileName = "nodes.json";

    private final File persistDir;

    private final ArrayList<Shard> shards = new ArrayList<>();

    /**
     * Returns the list of shards.
     *
     * @return the list of shards
     */
    public ArrayList<Shard> getShards() {
        return shards;
    }

    /**
     * Returns the shard that owns the given client slot.
     *
     * @param toClientSlot the client slot
     * @return the owning shard, or null if none
     */
    public Shard getShardBySlot(int toClientSlot) {
        for (var shard : shards) {
            if (shard.contains(toClientSlot)) {
                return shard;
            }
        }

        return null;
    }

    private static final Logger log = LoggerFactory.getLogger(MultiShard.class);

    /**
     * Constructs a MultiShard, loading persisted metadata and adding the local node if needed.
     *
     * @param persistDir the directory where cluster metadata is persisted
     * @throws IOException if loading or saving metadata fails
     */
    public MultiShard(File persistDir) throws IOException {
        this.persistDir = persistDir;
        loadMeta();
        addMySelfIfNeed();
    }

    /**
     * Resets all shards to only the local node, optionally resetting cluster epochs.
     *
     * @param isResetEpoch whether to reset the cluster epochs to zero
     * @throws IOException if saving metadata fails
     */
    public void reset(boolean isResetEpoch) throws IOException {
        shards.clear();
        addMySelfIfNeed();

        if (isResetEpoch) {
            clusterMyEpoch = 0;
            clusterCurrentEpoch = 0;
        }
    }

    private void addMySelfIfNeed() throws IOException {
        if (shards.isEmpty()) {
            var shard = new Shard();
            shards.add(shard);

            var node = new Node();
            var hostAndPort = ConfForGlobal.announcedHostPort();
//            if (hostAndPort == null) {
//                throw new RuntimeException("Repl clusterx parse host and port failed=" + ConfForGlobal.netListenAddress);
//            }
            node.setMaster(true);
            node.host = hostAndPort.host;
            node.port = hostAndPort.port;
            node.setMySelf(true);
            // with blank slot range

            shard.getNodes().add(node);

            saveMeta();
        }
    }

    /**
     * Returns the shard that contains the local node.
     *
     * @return the local node's shard, or null if none
     */
    public Shard mySelfShard() {
        for (var shard : shards) {
            for (var node : shard.getNodes()) {
                if (node.isMySelf()) {
                    return shard;
                }
            }
        }
        return null;
    }

    @VisibleForTesting
    synchronized void loadMeta() throws IOException {
        var metaFile = new File(persistDir, metaFileName);
        if (!metaFile.exists()) {
            log.warn("Repl clusterx meta file not found={}", metaFile);
            return;
        }

        var metaJson = FileUtils.readFileToString(metaFile, StandardCharsets.UTF_8);

        var objectMapper = new ObjectMapper();
        var tmp = objectMapper.readValue(metaJson, TmpForJson.class);
        shards.clear();
        shards.addAll(tmp.getShards());
        clusterMyEpoch = tmp.getClusterMyEpoch();
        clusterCurrentEpoch = tmp.getClusterCurrentEpoch();
        log.warn("Repl clusterx meta loaded, shards size={}", shards.size());

    }

    /**
     * Replaces all shards with the provided list and updates the cluster version.
     *
     * @param shardsNew     the new list of shards
     * @param clusterVersion the new cluster version (current epoch)
     * @throws IOException if saving metadata fails
     */
    public synchronized void refreshAllShards(ArrayList<Shard> shardsNew, int clusterVersion) throws IOException {
        shards.clear();
        shards.addAll(shardsNew);
        clusterCurrentEpoch = clusterVersion;
        saveMeta();
    }

    /**
     * Updates the cluster version and persists metadata.
     *
     * @param clusterVersion the new cluster version, or 0 to leave it unchanged
     * @throws IOException if saving metadata fails
     */
    public synchronized void updateClusterVersion(int clusterVersion) throws IOException {
        if (clusterVersion != 0) {
            clusterCurrentEpoch = clusterVersion;
        }

        saveMeta();
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
     * Returns the current cluster epoch.
     *
     * @return the cluster current epoch
     */
    public int getClusterCurrentEpoch() {
        return clusterCurrentEpoch;
    }

    private int clusterMyEpoch;

    private int clusterCurrentEpoch;

    /**
     * Persists the current cluster metadata to disk and re-initializes slots.
     *
     * @throws IOException if writing metadata fails
     */
    public synchronized void saveMeta() throws IOException {
        clusterMyEpoch++;

        var tmp = new TmpForJson();
        tmp.setShards(shards);
        tmp.setClusterMyEpoch(clusterMyEpoch);
        tmp.setClusterCurrentEpoch(clusterCurrentEpoch);

        var objectMapper = new ObjectMapper();
        var metaJson = objectMapper.writeValueAsString(tmp);

        var metaFile = new File(persistDir, metaFileName);
        FileUtils.writeStringToFile(metaFile, metaJson, StandardCharsets.UTF_8);
        log.warn("Repl clusterx meta saved, shards size={}", shards.size());

        var localPersist = LocalPersist.getInstance();
        localPersist.initSlotsAgainAfterMultiShardLoadedOrChanged();
    }

    /**
     * Returns the first (smallest) client slot across all shards.
     *
     * @return the first client slot, or null if none
     */
    public Integer firstToClientSlot() {
        Integer min = null;
        for (var shard : shards) {
            var list = shard.getMultiSlotRange().getList();
            if (list.isEmpty()) {
                continue;
            }

            var firstSlot = list.getFirst().getBegin();
            if (min == null || firstSlot < min) {
                min = firstSlot;
            }
        }
        return min;
    }

    /**
     * Returns the last (largest) client slot across all shards.
     *
     * @return the last client slot, or null if none
     */
    public Integer lastToClientSlot() {
        Integer max = null;
        for (var shard : shards) {
            var list = shard.getMultiSlotRange().getList();
            if (list.isEmpty()) {
                continue;
            }

            var lastSlot = list.getLast().getEnd();
            if (max == null || lastSlot > max) {
                max = lastSlot;
            }
        }
        return max;
    }

    /**
     * Returns the smallest client slot that is greater than the given client slot.
     *
     * @param toClientSlot the reference client slot
     * @return the next client slot, or null if none
     */
    public Integer nextToClientSlot(int toClientSlot) {
        Integer min = null;
        for (var shard : shards) {
            var list = shard.getMultiSlotRange().getList();
            if (list.isEmpty()) {
                continue;
            }

            var firstSlot = list.getFirst().begin;
            if (firstSlot > toClientSlot) {
                if (min == null || firstSlot < min) {
                    min = firstSlot;
                }
            }
        }
        return min;
    }
}
