package io.velo.persist.index

import org.rocksdb.CompressionType
import org.rocksdb.HashSkipListMemTableConfig
import org.rocksdb.Options
import org.rocksdb.RocksDB

RocksDB.loadLibrary()

def memTableConfig = new HashSkipListMemTableConfig()
memTableConfig.bucketCount = 10_000
memTableConfig.height = 4

def options = new Options()
        .setCreateIfMissing(true)
        .setCompressionType(CompressionType.NO_COMPRESSION)
        .setNumLevels(2)
        .setLevelZeroFileNumCompactionTrigger(8)
        .setMaxOpenFiles(64)
        .setMaxBackgroundJobs(4)

def db = RocksDB.open(options, '/tmp/test_rocksdb_config')

final byte[] valueLengthAsInt = new byte[4]

10.times { nn ->
    long beginT = System.currentTimeMillis()
    100_000_000.times {
        def key = 'key:' + (it.toString().padLeft(12, '0'))
        db.put(key.bytes, valueLengthAsInt)

        if (it % 1000000 == 0) {
            println "Inserted $it keys"
        }
    }
    long endT = System.currentTimeMillis()
    println "Insert 100_000_000 keys cost ${endT - beginT} ms"
}

Thread.sleep(1000 * 10)
db.close()