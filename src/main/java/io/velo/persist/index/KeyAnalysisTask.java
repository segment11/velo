package io.velo.persist.index;

import io.velo.persist.LocalPersist;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class KeyAnalysisTask implements KeyAnalysisHandler.InnerTask {
    private final RocksDB db;

    private static final Logger log = LoggerFactory.getLogger(KeyAnalysisTask.class);

    public KeyAnalysisTask(RocksDB db) {
        this.db = db;
    }

    @Override
    public void run(int loopCount) {
        if (LocalPersist.getInstance().isDebugMode()) {
            log.info("Key analysis task loop count: {}", loopCount);
        }
    }
}
