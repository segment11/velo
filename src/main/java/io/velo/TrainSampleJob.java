package io.velo;

import com.github.luben.zstd.ZstdDictTrainer;
import com.github.luben.zstd.ZstdException;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.stream.Collectors;

/**
 * A class responsible for training sample data to generate dictionaries using the Zstd compression library.
 * Each instance of this class is associated with a worker ID, thread local safe.
 */
public class TrainSampleJob {
    /**
     * The unique identifier for the worker.
     */
    private final byte workerId;

    /**
     * The number of train operations performed. This field is visible for testing purposes.
     */
    @VisibleForTesting
    int trainCount = 0;

    /**
     * Constructs a new instance of TrainSampleJob with the given worker ID.
     *
     * @param workerId the unique identifier for the worker
     */
    public TrainSampleJob(byte workerId) {
        this.workerId = workerId;
    }

    /**
     * Logger for logging information and errors.
     */
    private static final Logger log = LoggerFactory.getLogger(TrainSampleJob.class);

    /**
     * The minimum number of samples required to start training a dictionary.
     */
    public static final int MIN_TRAIN_SAMPLE_SIZE = 10;

    /**
     * A cache that stores trained dictionaries by their key prefix or suffix.
     */
    private final HashMap<String, Dict> cacheDict = new HashMap<>();

    /**
     * A copy of the list of samples to be trained.
     */
    private List<TrainSampleKV> sampleToTrainListCopy = new ArrayList<>();

    /**
     * A list of sequence numbers of samples that have been removed after training.
     */
    private final List<Long> removedSampleKVSeqList = new ArrayList<>();

    /**
     * Resets the list of samples to be trained with a new list.
     *
     * @param list the new list of samples
     */
    public void resetSampleToTrainList(List<TrainSampleKV> list) {
        sampleToTrainListCopy = new ArrayList<>(list);
        removedSampleKVSeqList.clear();
    }

    /**
     * The size of the dictionary to be trained.
     */
    private int dictSize = 1024;

    /**
     * Sets the size of the dictionary to be trained.
     *
     * @param dictSize the size of the dictionary
     */
    public void setDictSize(int dictSize) {
        this.dictSize = dictSize;
    }

    /**
     * The minimum body length of samples required to start training a dictionary.
     */
    private int trainSampleMinBodyLength = 4096;

    /**
     * Sets the minimum body length of samples required to start training a dictionary.
     *
     * @param trainSampleMinBodyLength the minimum body length of samples
     */
    public void setTrainSampleMinBodyLength(int trainSampleMinBodyLength) {
        this.trainSampleMinBodyLength = trainSampleMinBodyLength;
    }

    /**
     * The exclusive end index for the dictionary key prefix.
     */
    private static int dictKeyPrefixEndIndex = 5;

    /**
     * Sets the exclusive end index for the dictionary key prefix.
     *
     * @param dictKeyPrefixEndIndex the end index for the dictionary key prefix
     */
    public static void setDictKeyPrefixEndIndex(int dictKeyPrefixEndIndex) {
        TrainSampleJob.dictKeyPrefixEndIndex = dictKeyPrefixEndIndex;
    }

    /**
     * The key used in dynamic configuration for dictionary key prefix or suffix groups.
     */
    public static final String KEY_IN_DYN_CONFIG = "dict_key_prefix_or_suffix_groups";

    /**
     * The list of key prefix or suffix groups.
     */
    private static ArrayList<String> keyPrefixOrSuffixGroupList = new ArrayList<>();

    /**
     * Returns the list of key prefix or suffix groups.
     *
     * @return the list of key prefix or suffix groups
     */
    public static ArrayList<String> getKeyPrefixOrSuffixGroupList() {
        return keyPrefixOrSuffixGroupList;
    }

    /**
     * Sets the list of key prefix or suffix groups.
     *
     * @param keyPrefixOrSuffixGroupList the list of key prefix or suffix groups
     */
    public synchronized static void setKeyPrefixOrSuffixGroupList(ArrayList<String> keyPrefixOrSuffixGroupList) {
        // longer first
        keyPrefixOrSuffixGroupList.sort((a, b) -> b.length() - a.length());
        TrainSampleJob.keyPrefixOrSuffixGroupList = keyPrefixOrSuffixGroupList;
    }

    /**
     * Adds a key prefix group to the list if it does not already exist.
     *
     * @param keyPrefixOrSuffixGroup the key prefix group to add
     */
    public synchronized static void addKeyPrefixGroupIfNotExist(String keyPrefixOrSuffixGroup) {
        if (keyPrefixOrSuffixGroupList.contains(keyPrefixOrSuffixGroup)) {
            return;
        }
        keyPrefixOrSuffixGroupList.add(keyPrefixOrSuffixGroup);
        // longer first
        keyPrefixOrSuffixGroupList.sort((a, b) -> b.length() - a.length());
    }

    /**
     * Trains a new dictionary using the provided list of samples.
     *
     * @param list the list of samples to be used for training
     * @return the trained dictionary, or null if training fails
     */
    private Dict trainNewDict(List<TrainSampleKV> list) {
        int sampleBodyLength = 0;
        int sampleNum = 0;
        List<TrainSampleKV> trainSampleList = new ArrayList<>();
        for (var one : list) {
            sampleBodyLength += one.valueBytes().length;
            sampleNum += 1;
            trainSampleList.add(one);

            if (sampleBodyLength >= trainSampleMinBodyLength && sampleNum > MIN_TRAIN_SAMPLE_SIZE) {
                break;
            }
        }

        // list is not empty, sampleBodyLength > 0
        var trainer = new ZstdDictTrainer(sampleBodyLength, dictSize);
        for (var one : trainSampleList) {
            var body = one.valueBytes();
            boolean isAddSampleOk = trainer.addSample(body);
            assert isAddSampleOk;
        }

        byte[] dictBytes;
        try {
            var beginT = System.currentTimeMillis();
            dictBytes = trainer.trainSamples();
            var costT = System.currentTimeMillis() - beginT;

            log.info("Train sample, w={} train dict ok, sample size={}, dict size={}, cost time={} ms",
                    workerId, sampleBodyLength, dictBytes.length, costT);
        } catch (ZstdException ze) {
            log.error("Train sample, w={} train dict, sample size={}, error={}",
                    workerId, sampleBodyLength, ze.getMessage());
            return null;
        }

        return new Dict(dictBytes);
    }

    /**
     * Determines the key prefix or suffix for a given key.
     *
     * @param key the key for which to determine the prefix or suffix
     * @return the key prefix or suffix
     */
    public static String keyPrefixOrSuffixGroup(String key) {
        if (!keyPrefixOrSuffixGroupList.isEmpty()) {
            for (var keyPrefixOrSuffix : keyPrefixOrSuffixGroupList) {
                if (key.startsWith(keyPrefixOrSuffix) || key.endsWith(keyPrefixOrSuffix)) {
                    return keyPrefixOrSuffix;
                }
            }
        }

        // todo, maybe not good
        // prefer to use last index of '.' or ':'
        var index = key.lastIndexOf('.');
        if (index != -1) {
            return key.substring(0, index);
        } else {
            var index2 = key.lastIndexOf(':');
            if (index2 != -1) {
                // include :
                return key.substring(0, index2 + 1);
            } else {
                return key.substring(0, Math.min(key.length(), dictKeyPrefixEndIndex));
            }
        }
    }

    private final Debug debug = Debug.getInstance();

    /**
     * Trains dictionaries for the samples in the list.
     *
     * @return the result of the training process, including the trained dictionaries and the list of removed sample sequence numbers
     */
    public TrainSampleResult train() {
        trainCount++;
        if (trainCount % 100 == 0 || debug.logTrainDict) {
            log.info("Train sample, worker {} train sample list size={}, dict size={}, i am alive",
                    workerId, sampleToTrainListCopy.size(), cacheDict.size());
        }

        if (sampleToTrainListCopy.size() <= MIN_TRAIN_SAMPLE_SIZE) {
            return null;
        }

        var groupByKeyPrefixOrSuffixMap = sampleToTrainListCopy.stream().collect(Collectors.groupingBy(one -> {
            if (one.keyPrefixOrSuffixGiven != null) {
                return one.keyPrefixOrSuffixGiven;
            }

            var key = one.key();
            return keyPrefixOrSuffixGroup(key);
        }));

        for (var entry : groupByKeyPrefixOrSuffixMap.entrySet()) {
            var keyPrefixOrSuffix = entry.getKey();
            var list = entry.getValue();
            var dict = cacheDict.get(keyPrefixOrSuffix);
            if (dict != null) {
                for (var one : list) {
                    removedSampleKVSeqList.add(one.seq());
                }
                continue;
            }

            if (list.size() <= MIN_TRAIN_SAMPLE_SIZE) {
                // for next time
                continue;
            }

            dict = trainNewDict(list);
            if (dict != null) {
                // in one thread, no need lock
                cacheDict.put(keyPrefixOrSuffix, dict);

                // remove trained sample
                for (var one : list) {
                    removedSampleKVSeqList.add(one.seq());
                }
                log.info("Train sample, worker {} train dict ok, key prefix={}, dict size={}, removed sample size={}",
                        workerId, keyPrefixOrSuffix, dict.getDictBytes().length, list.size());

                // need persist immediately, todo
            }
        }
        return new TrainSampleResult(new HashMap<>(cacheDict), new ArrayList<>(removedSampleKVSeqList));
    }

    /**
     * A record representing the result of the training process.
     * Contains the trained dictionaries and the list of removed sample sequence numbers.
     */
    public record TrainSampleResult(HashMap<String, Dict> cacheDict, ArrayList<Long> removedSampleKVSeqList) {
    }

    /**
     * A record representing a sample key-value pair.
     * Contains the key, an optional key prefix or suffix, the sequence number, and the value bytes.
     */
    public record TrainSampleKV(String key, String keyPrefixOrSuffixGiven, Long seq, byte[] valueBytes) {
    }
}