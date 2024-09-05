package io.velo.repl.cluster;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;

public class Shard {
    static final String metaFileName = "nodes.json";

    private final MultiSlotRange multiSlotRange = new MultiSlotRange();
    private final ArrayList<ShardNode> nodes = new ArrayList<>();
    private final File persistDir;

    private static final Logger log = LoggerFactory.getLogger(Shard.class);

    public Shard(File persistDir) throws IOException {
        this.persistDir = persistDir;
        loadMeta();
    }

    @VisibleForTesting
    void loadMeta() throws IOException {
        var metaFile = new File(persistDir, metaFileName);
        if (!metaFile.exists()) {
            log.warn("Repl clusterx meta file not found: {}", metaFile);
            return;
        }

        var metaJson = FileUtils.readFileToString(metaFile, StandardCharsets.UTF_8);

        var objectMapper = new ObjectMapper();
        var m0 = objectMapper.readValue(metaJson, TmpForJson.class);
        multiSlotRange.list.addAll(m0.getList());
        nodes.addAll(m0.getNodes());
    }

    public void saveMeta() throws IOException {
        var metaFile = new File(persistDir, metaFileName);
        var objectMapper = new ObjectMapper();
        var m0 = new TmpForJson();
        m0.setList(multiSlotRange.list);
        m0.setNodes(nodes);
        var metaJson = objectMapper.writeValueAsString(m0);
        FileUtils.writeStringToFile(metaFile, metaJson, StandardCharsets.UTF_8);
    }

    public ShardNode master() {
        for (var node : nodes) {
            if (node.isMaster) {
                return node;
            }
        }
        return null;
    }

    public ShardNode slave(int slaveIndex) {
        for (var node : nodes) {
            if (!node.isMaster && node.slaveIndex == slaveIndex) {
                return node;
            }
        }
        return null;
    }
}
