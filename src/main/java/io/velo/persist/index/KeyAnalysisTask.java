package io.velo.persist.index;

import org.rocksdb.RocksDB;

public class KeyAnalysisTask implements KeyAnalysisHandler.InnerTask {
    private final RocksDB db;

    public KeyAnalysisTask(RocksDB db) {
        this.db = db;
    }

    @Override
    public void run(int loopCount) {

    }
}
