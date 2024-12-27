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

public class DictMap implements NeedCleanUp {
    public static int TO_COMPRESS_MIN_DATA_LENGTH = 64;
    public static int TO_COMPRESS_USE_SELF_DICT_MIN_DATA_LENGTH = 256;

    @VisibleForTesting
    final static SimpleGauge dictCompressedGauge = new SimpleGauge("dict", "Dict compressed metrics.", "name");

    static {
        dictCompressedGauge.register();
    }

    // singleton
    private static final DictMap instance = new DictMap();

    public static DictMap getInstance() {
        return instance;
    }

    private DictMap() {
        initMetricsCollect();
    }

    private Binlog binlog;

    public void setBinlog(Binlog binlog) {
        this.binlog = binlog;
    }

    private static final Logger log = LoggerFactory.getLogger(DictMap.class);

    public Dict getDictBySeq(int seq) {
        return cacheDictBySeq.get(seq);
    }

    public Dict getDict(String keyPrefixOrSuffix) {
        return cacheDict.get(keyPrefixOrSuffix);
    }

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

    public HashMap<String, Dict> getCacheDictCopy() {
        return new HashMap<>(cacheDict);
    }

    public TreeMap<Integer, Dict> getCacheDictBySeqCopy() {
        return new TreeMap<>(cacheDictBySeq);
    }

    // worker share dict, init on start, need persist
    // for compress
    private final ConcurrentHashMap<String, Dict> cacheDict = new ConcurrentHashMap<>();
    // can not be removed
    // for decompress
    // if dict retrain, and dict count is large, it will be a problem, need clean not used dict, todo
    private final ConcurrentHashMap<Integer, Dict> cacheDictBySeq = new ConcurrentHashMap<>();

    public int dictSize() {
        return cacheDictBySeq.size();
    }

    private FileOutputStream fos;

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

    private static final String FILE_NAME = "dict-map.dat";
    private File dirFile;

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

    public synchronized void updateGlobalDictBytes(byte[] dictBytes) {
        Dict.resetGlobalDictBytes(dictBytes);
        Dict.saveGlobalDictBytesToFile(new File(dirFile, Dict.GLOBAL_DICT_FILE_NAME));
    }

    private final List<String> labelValuesGlobal = List.of("global_");
    private final List<String> labelValuesSelf = List.of("self_");

    private void initMetricsCollect() {
        // only first slot show global metrics
        dictCompressedGauge.addRawGetter(() -> {
            var map = new HashMap<String, SimpleGauge.ValueWithLabelValues>();

            for (var entry : cacheDict.entrySet()) {
                var dict = entry.getValue();
                var labelValues = List.of(entry.getKey());

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
