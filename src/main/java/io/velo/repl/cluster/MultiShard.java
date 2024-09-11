package io.velo.repl.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import io.velo.ConfForGlobal;
import io.velo.repl.ReplPair;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class MultiShard {
    public static final short TO_CLIENT_SLOT_NUMBER = 16384;

    public static short asInnerSlotByToClientSlot(int toClientSlot) {
        return (short) (toClientSlot / (TO_CLIENT_SLOT_NUMBER / ConfForGlobal.slotNumber));
    }

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

    public ArrayList<Shard> getShards() {
        return shards;
    }

    private static final Logger log = LoggerFactory.getLogger(MultiShard.class);

    public MultiShard(File persistDir) throws IOException {
        this.persistDir = persistDir;
        loadMeta();
        addMySelfIfNeed();
    }

    private void addMySelfIfNeed() throws IOException {
        if (shards.isEmpty()) {
            var shard = new Shard();
            shards.add(shard);

            var node = new Node();
            var hostAndPort = ReplPair.parseHostAndPort(ConfForGlobal.netListenAddresses);
//            if (hostAndPort == null) {
//                throw new RuntimeException("Repl clusterx parse host and port failed: " + ConfForGlobal.netListenAddresses);
//            }
            node.setMaster(true);
            node.host = hostAndPort.host();
            node.port = hostAndPort.port();
            node.setMySelf(true);
            // with blank slot range

            shard.getNodes().add(node);

            saveMeta();
        }
    }

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

    public synchronized void refreshAllShards(ArrayList<Shard> shardsNew, int clusterVersion) throws IOException {
        shards.clear();
        shards.addAll(shardsNew);
        clusterCurrentEpoch = clusterVersion;
        saveMeta();
    }

    public synchronized void updateClusterVersion(int clusterVersion) throws IOException {
        if (clusterVersion != 0) {
            clusterCurrentEpoch = clusterVersion;
        }

        clusterMyEpoch++;
        saveMeta();
    }

    public int getClusterMyEpoch() {
        return clusterMyEpoch;
    }

    public int getClusterCurrentEpoch() {
        return clusterCurrentEpoch;
    }

    private int clusterMyEpoch;

    private int clusterCurrentEpoch;

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
    }
}
