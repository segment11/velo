package io.velo;

import io.velo.metric.SimpleGauge;
import io.velo.persist.LocalPersist;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
import io.velo.repl.incremental.XDict;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Manages a map of dictionaries used for compression and decompression.
 * This class provides methods for adding, retrieving, and managing dictionaries, as well as handling metrics and persistence.
 */
public class DictMap implements NeedCleanUp {

    /**
     * Minimum data length to trigger compression.
     */
    public static int TO_COMPRESS_MIN_DATA_LENGTH = 64;

    /**
     * Minimum data length to use the self-Zstd dictionary for compression.
     */
    public static int TO_COMPRESS_USE_SELF_DICT_MIN_DATA_LENGTH = 256;

    /**
     * Gauge for tracking dictionary compression metrics.
     */
    @VisibleForTesting
    final static SimpleGauge dictCompressedGauge = new SimpleGauge("dict", "Dict compressed metrics.", "name");

    static {
        dictCompressedGauge.register();
    }

    /**
     * Singleton instance of DictMap.
     */
    private static final DictMap instance = new DictMap();

    /**
     * Returns the singleton instance of DictMap.
     *
     * @return the singleton instance of DictMap
     */
    public static DictMap getInstance() {
        return instance;
    }

    /**
     * Private constructor to enforce the singleton pattern.
     */
    private DictMap() {
        initMetricsCollect();
    }

    /**
     * Logger for logging information and errors.
     */
    private static final Logger log = LoggerFactory.getLogger(DictMap.class);

    /**
     * Retrieves a dictionary by its sequence number.
     *
     * @param seq the sequence number of the dictionary
     * @return the dictionary with the specified sequence number, or null if not found
     */
    public Dict getDictBySeq(int seq) {
        return cacheDictBySeq.get(seq);
    }

    /**
     * Retrieves a dictionary by its key prefix or suffix.
     *
     * @param keyPrefixOrSuffix the key prefix or suffix of the dictionary
     * @return the dictionary with the specified key prefix or suffix, or null if not found
     */
    public Dict getDict(String keyPrefixOrSuffix) {
        return cacheDict.get(keyPrefixOrSuffix);
    }

    private final LocalPersist localPersist = LocalPersist.getInstance();

    /**
     * @param keyPrefixOrSuffix the key prefix or suffix of the dictionary
     * @param dict              the dictionary to put
     * @return the dictionary that was put into the map
     */
    @SlaveNeedReplay
    @SlaveReplay
    public synchronized Dict putDict(String keyPrefixOrSuffix, Dict dict) {
        // The reserved SELF_ZSTD_DICT_SEQ is owned by the self-dict singleton.
        // generateRandomSeq() never returns 1 after the round-2 fix, but a caller
        // (e.g. corrupted binlog replay, hand-crafted test dict) could still force
        // it via setSeq(). Fail fast — do not silently regenerate, because the
        // value would already be on disk and the file write below would corrupt
        // the dict-map.
        assert dict.getSeq() != Dict.SELF_ZSTD_DICT_SEQ;

        // check dict seq is already in cache
        var existDict = cacheDictBySeq.get(dict.getSeq());
        if (existDict != null) {
            // generate new seq
            dict.setSeq(Dict.generateRandomSeq());
            // check again
            var existDict2 = cacheDictBySeq.get(dict.getSeq());
            if (existDict2 != null) {
                throw new RuntimeException("Dict seq conflict, dict seq=" + dict.getSeq());
            }
        }

        // 1. Write to file first — if this fails the dict is rejected outright
        try {
            fos.write(dict.encode(keyPrefixOrSuffix));
        } catch (IOException e) {
            throw new RuntimeException("Write dict to file error", e);
        }

        // 2. Update the in-memory cache BEFORE the binlog. The dict is already
        // persisted to disk above, so it is usable for compression/decompression
        // in this session regardless of binlog status.
        TrainSampleJob.addKeyPrefixGroupIfNotExist(keyPrefixOrSuffix);
        dict.initCtx();
        cacheDictBySeq.put(dict.getSeq(), dict);
        cacheDict.put(keyPrefixOrSuffix, dict);

        // 3. Append to binlog last. A binlog failure here is a REPLICATION
        // event (the slave will not see this dict), but it is NOT a put failure —
        // the dict is already in the cache and on disk. Log loudly and let
        // operators detect the replication drift via metrics/alerting. Do not
        // rethrow: rethrowing would leave the cache in an inconsistent state
        // (file written, cache not updated) and confuse the caller.
        var firstOneSlot = localPersist.firstOneSlot();
        if (firstOneSlot != null && firstOneSlot.getDynConfig().isBinlogOn()) {
            var p = firstOneSlot.asyncCall(() -> {
                try {
                    firstOneSlot.getBinlog().append(new XDict(keyPrefixOrSuffix, dict));
                    return true;
                } catch (IOException e) {
                    throw new RuntimeException("Append binlog error, dict key prefix=" + keyPrefixOrSuffix, e);
                }
            });
            var r = p.getResult();
            var e = p.getException();
            if (e != null) {
                // Master-slave divergence: dict is in the master's cache and
                // dict-map.dat, but no binlog entry was created, so the slave
                // will not receive it. Operators must repair via re-train or
                // a manual xdict sync.
                log.error("Append binlog error (dict is in master cache but not replicated to slave), " +
                        "dict key prefix={}, seq={}", keyPrefixOrSuffix, dict.getSeq(), e);
            } else {
                log.warn("Append binlog success, dict key prefix={}, result={}", keyPrefixOrSuffix, r);
            }
        }

        return dict;
    }

    /**
     * Returns a copy of the cache dictionary map.
     *
     * @return a copy of the cache dictionary map
     */
    public HashMap<String, Dict> getCacheDictCopy() {
        return new HashMap<>(cacheDict);
    }

    /**
     * Returns a copy of the cache dictionary by sequence map.
     *
     * @return a copy of the cache dictionary by sequence map
     */
    public TreeMap<Integer, Dict> getCacheDictBySeqCopy() {
        return new TreeMap<>(cacheDictBySeq);
    }

    /**
     * Cache for dictionaries by key prefix or suffix.
     */
    private final ConcurrentHashMap<String, Dict> cacheDict = new ConcurrentHashMap<>();

    /**
     * Cache for dictionaries by sequence number.
     */
    private final ConcurrentHashMap<Integer, Dict> cacheDictBySeq = new ConcurrentHashMap<>();

    /**
     * Returns the size of the dictionary map.
     *
     * @return the size of the dictionary map
     */
    public int dictSize() {
        return cacheDictBySeq.size();
    }

    /**
     * File output stream for writing dictionaries to a file.
     */
    private FileOutputStream fos;

    /**
     * Cleans up resources used by the dictionary map.
     */
    @Override
    public synchronized void cleanUp() {
        if (fos != null) {
            // finally block ensures fos is always nulled, even if close() throws.
            // Without it, a transient I/O error during shutdown leaves fos pointing
            // at a closed-or-broken stream, and the next cleanUp() / putDict()
            // would call into a closed stream (silently no-op'd or throws).
            // Use System.out/err directly: the SLF4J logger may already be
            // shut down at this point in the server-stop sequence.
            try {
                fos.close();
                System.out.println("Close dict fos");
            } catch (IOException e) {
                System.err.println("Close dict fos error, message=" + e.getMessage());
            } finally {
                fos = null;
            }
        }

        for (var entry : cacheDictBySeq.entrySet()) {
            entry.getValue().closeCtx();
        }
    }

    /**
     * Clears all dictionaries from the map and truncates the dictionary file.
     */
    public synchronized void clearAll() {
        for (var entry : cacheDictBySeq.entrySet()) {
            entry.getValue().closeCtx();
        }

        cacheDict.clear();
        cacheDictBySeq.clear();

        // truncate file
        try {
            fos.getChannel().truncate(0);
            System.out.println("Truncate dict file");
        } catch (IOException e) {
            throw new RuntimeException("Truncate dict file error", e);
        }
    }

    /**
     * File name for the dictionary map file.
     */
    private static final String FILE_NAME = "dict-map.dat";

    /**
     * Directory file for storing dictionary files.
     */
    private File dirFile;

    /**
     * @param dirFile the directory file to initialize from
     */
    public synchronized void initDictMap(File dirFile) throws IOException {
        this.dirFile = dirFile;
        var file = new File(dirFile, FILE_NAME);
        if (!file.exists()) {
            FileUtils.touch(file);
        }

        this.fos = new FileOutputStream(file, true);

        int n = 0;
        ArrayList<Integer> loadedSeqList = new ArrayList<>();
        // fix: wrap in try-with-resources so the DataInputStream (and underlying FileInputStream) is always closed.
        // previously the stream was never closed, leaking a file descriptor on every call.
        if (file.length() > 0) {
            try (var is = new DataInputStream(new FileInputStream(file))) {
                while (true) {
                    var dictWithKey = Dict.decode(is);
                    if (dictWithKey == null) {
                        break;
                    }

                    var dict = dictWithKey.dict();
                    dict.initCtx();

                    cacheDictBySeq.put(dict.getSeq(), dict);
                    cacheDict.put(dictWithKey.keyPrefixOrSuffix(), dict);

                    loadedSeqList.add(dict.getSeq());
                    n++;
                }
            }
        }

        log.info("Dict map init, map size={}, seq map size={}, n={}, loaded seq list={}",
                cacheDict.size(), cacheDictBySeq.size(), n, loadedSeqList);

        // add exists dict key prefix or suffix as train sample key prefix or suffix group, so new request values can use exist dict directly
        for (var entry : cacheDict.entrySet()) {
            var keyPrefixOrSuffix = entry.getKey();
            TrainSampleJob.addKeyPrefixGroupIfNotExist(keyPrefixOrSuffix);
        }
    }

    /**
     * Label values for self dictionary metrics.
     */
    private final List<String> labelValuesSelf = List.of("self_");

    /**
     * Initializes the metrics collection for dictionary compression.
     */
    private void initMetricsCollect() {
        // only the first slot shows global metrics
        dictCompressedGauge.addRawGetter(() -> {
            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

            for (var entry : cacheDict.entrySet()) {
                var keyPrefixOrSuffix = entry.getKey();
                var dict = entry.getValue();
                var labelValues = List.of(keyPrefixOrSuffix);

                var compressedCount = dict.compressedCountTotal.sum();
                var compressedRatio = dict.compressedRatio();

                map.put("dict_compressed_count_" + dict.getSeq(), new SimpleGauge.ValueWithLabelValues((double) compressedCount, labelValues));
                map.put("dict_compressed_ratio_" + dict.getSeq(), new SimpleGauge.ValueWithLabelValues(compressedRatio, labelValues));
            }

            // self dict
            var compressedCount = Dict.SELF_ZSTD_DICT.compressedCountTotal.sum();
            var compressedRatio = Dict.SELF_ZSTD_DICT.compressedRatio();

            map.put("dict_compressed_count_" + Dict.SELF_ZSTD_DICT_SEQ, new SimpleGauge.ValueWithLabelValues((double) compressedCount, labelValuesSelf));
            map.put("dict_compressed_ratio_" + Dict.SELF_ZSTD_DICT_SEQ, new SimpleGauge.ValueWithLabelValues(compressedRatio, labelValuesSelf));

            return map;
        });
    }
}
