package io.velo;

import io.velo.metric.SimpleGauge;
import io.velo.repl.Binlog;
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
     * Binlog for logging dictionary changes.
     */
    private Binlog binlog;

    /**
     * Sets the binlog for logging dictionary changes.
     *
     * @param binlog the binlog to set
     */
    public void setBinlog(Binlog binlog) {
        this.binlog = binlog;
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

    /**
     * Puts a dictionary into the map with a key prefix or suffix.
     * If the dictionary's sequence number already exists, a new sequence number is generated.
     *
     * @param keyPrefixOrSuffix the key prefix or suffix of the dictionary
     * @param dict              the dictionary to put
     * @return the dictionary that was put into the map
     * @throws RuntimeException if there is an error writing the dictionary to the file or appending to the binlog
     */
    @SlaveNeedReplay
    @SlaveReplay
    public synchronized Dict putDict(String keyPrefixOrSuffix, Dict dict) {
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

        try {
            fos.write(dict.encode(keyPrefixOrSuffix));
        } catch (IOException e) {
            throw new RuntimeException("Write dict to file error", e);
        }

        if (binlog != null) {
            try {
                binlog.append(new XDict(keyPrefixOrSuffix, dict));
            } catch (IOException e) {
                throw new RuntimeException("Append binlog error, dict key prefix=" + keyPrefixOrSuffix, e);
            }
        }

        TrainSampleJob.addKeyPrefixGroupIfNotExist(keyPrefixOrSuffix);

        dict.initCtx();

        cacheDictBySeq.put(dict.getSeq(), dict);
        return cacheDict.put(keyPrefixOrSuffix, dict);
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
            try {
                fos.close();
                System.out.println("Close dict fos");
                fos = null;
            } catch (IOException e) {
                System.err.println("Close dict fos error, message=" + e.getMessage());
            }
        }

        for (var entry : cacheDictBySeq.entrySet()) {
            entry.getValue().closeCtx();
        }
        Dict.GLOBAL_ZSTD_DICT.closeCtx();
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
     * Initializes the dictionary map from a directory file.
     *
     * @param dirFile the directory file to initialize from
     * @throws IOException if an I/O error occurs
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
        if (file.length() > 0) {
            var is = new DataInputStream(new FileInputStream(file));
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

        log.info("Dict map init, map size={}, seq map size={}, n={}, loaded seq list={}",
                cacheDict.size(), cacheDictBySeq.size(), n, loadedSeqList);

        // add exists dict key prefix or suffix as train sample key prefix or suffix group, so new request values can use exist dict directly
        for (var entry : cacheDict.entrySet()) {
            var keyPrefixOrSuffix = entry.getKey();
            TrainSampleJob.addKeyPrefixGroupIfNotExist(keyPrefixOrSuffix);
        }

        Dict.initGlobalDictBytesByFile(new File(dirFile, Dict.GLOBAL_DICT_FILE_NAME));
    }

    /**
     * Updates the global dictionary bytes and saves them to a file.
     *
     * @param dictBytes the new global dictionary bytes
     */
    public synchronized void updateGlobalDictBytes(byte[] dictBytes) {
        Dict.resetGlobalDictBytes(dictBytes);
        Dict.saveGlobalDictBytesToFile(new File(dirFile, Dict.GLOBAL_DICT_FILE_NAME));
    }

    /**
     * Label values for global dictionary metrics.
     */
    private final List<String> labelValuesGlobal = List.of("global_");

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

            if (Dict.GLOBAL_ZSTD_DICT.hasDictBytes()) {
                var compressedCount = Dict.GLOBAL_ZSTD_DICT.compressedCountTotal.sum();
                var compressedRatio = Dict.GLOBAL_ZSTD_DICT.compressedRatio();

                map.put("dict_compressed_count_" + Dict.GLOBAL_ZSTD_DICT_SEQ, new SimpleGauge.ValueWithLabelValues((double) compressedCount, labelValuesGlobal));
                map.put("dict_compressed_ratio_" + Dict.GLOBAL_ZSTD_DICT_SEQ, new SimpleGauge.ValueWithLabelValues(compressedRatio, labelValuesGlobal));
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

