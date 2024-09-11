package io.velo.persist;

import io.velo.CompressedValue;
import io.velo.Debug;
import io.velo.metric.InSlotMetricCollector;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.incremental.XOneWalGroupPersist;
import org.apache.lucene.util.RamUsageEstimator;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;
import java.util.stream.Collectors;

public class ChunkMergeWorker implements InMemoryEstimate, InSlotMetricCollector {
    private final short slot;
    final OneSlot oneSlot;

    // metrics
    long mergedSegmentCount = 0;
    long mergedSegmentCostTimeTotalUs = 0;
    int lastMergedSegmentIndex = -1;

    long validCvCountTotal = 0;
    long invalidCvCountTotal = 0;

    private static final Logger log = LoggerFactory.getLogger(ChunkMergeWorker.class);

    record CvWithKeyAndBucketIndexAndSegmentIndex(CompressedValue cv, String key, int bucketIndex, int segmentIndex) {
    }

    // for better latency, because group by wal group, if wal groups is too large, need multi batch persist
    @VisibleForTesting
    int MERGED_SEGMENT_SIZE_THRESHOLD = 256;
    @VisibleForTesting
    int MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST = 8;
    @VisibleForTesting
    int MERGED_CV_SIZE_THRESHOLD = 256 * 64;

    void resetThreshold(int walGroupNumber) {
        MERGED_SEGMENT_SIZE_THRESHOLD = Math.min(walGroupNumber, 256);
        MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST = Math.max(MERGED_SEGMENT_SIZE_THRESHOLD / 32, 4);
        MERGED_CV_SIZE_THRESHOLD = MERGED_SEGMENT_SIZE_THRESHOLD * 64;

        log.info("Reset chunk merge worker threshold, wal group number={}, merged segment size threshold={}, " +
                        "merged segment size threshold once persist={}, merged cv size threshold={}",
                walGroupNumber, MERGED_SEGMENT_SIZE_THRESHOLD, MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST, MERGED_CV_SIZE_THRESHOLD);

        final var maybeOneMergedCvBytesLength = 200;
        var lruMemoryRequireMB = MERGED_CV_SIZE_THRESHOLD * maybeOneMergedCvBytesLength / 1024 / 1024;
        log.info("LRU max size for chunk segment merged cv buffer={}, maybe one merged cv bytes length is {}B, memory require={}MB, slot: {}",
                MERGED_CV_SIZE_THRESHOLD,
                maybeOneMergedCvBytesLength,
                lruMemoryRequireMB,
                slot);
        log.info("LRU prepare, type: {}, MB: {}, slot: {}", LRUPrepareBytesStats.Type.chunk_segment_merged_cv_buffer, lruMemoryRequireMB, slot);
        LRUPrepareBytesStats.add(LRUPrepareBytesStats.Type.chunk_segment_merged_cv_buffer, String.valueOf(slot), lruMemoryRequireMB, false);
    }

    private final List<CvWithKeyAndBucketIndexAndSegmentIndex> mergedCvList = new ArrayList<>(MERGED_CV_SIZE_THRESHOLD);

    void addMergedCv(CvWithKeyAndBucketIndexAndSegmentIndex cvWithKeyAndBucketIndexAndSegmentIndex) {
        mergedCvList.add(cvWithKeyAndBucketIndexAndSegmentIndex);
    }

    @VisibleForTesting
    int getMergedCvListSize() {
        return mergedCvList.size();
    }

    @TestOnly
    void clearMergedCvList() {
        mergedCvList.clear();
    }

    @VisibleForTesting
    record MergedSegment(int segmentIndex, int validCvCount) implements Comparable<MergedSegment> {
        @Override
        public String toString() {
            return "S{" +
                    ", s=" + segmentIndex +
                    ", count=" + validCvCount +
                    '}';
        }

        @Override
        public int compareTo(@NotNull ChunkMergeWorker.MergedSegment o) {
            return this.segmentIndex - o.segmentIndex;
        }
    }

    private final TreeSet<MergedSegment> mergedSegmentSet = new TreeSet<>();

    void addMergedSegment(int segmentIndex, int validCvCount) {
        mergedSegmentSet.add(new MergedSegment(segmentIndex, validCvCount));
    }

    boolean isMergedSegmentSetEmpty() {
        return mergedSegmentSet.isEmpty();
    }

    @VisibleForTesting
    int getMergedSegmentSetSize() {
        return mergedSegmentSet.size();
    }

    // not empty when call this
    int firstMergedSegmentIndex() {
        return mergedSegmentSet.first().segmentIndex;
    }

    @TestOnly
    void clearMergedSegmentSet() {
        mergedSegmentSet.clear();
    }

    OneSlot.BeforePersistWalExt2FromMerge getMergedButNotPersistedBeforePersistWal(int walGroupIndex) {
        if (mergedCvList.isEmpty()) {
            return null;
        }

        ArrayList<Integer> segmentIndexList = new ArrayList<>();

        ArrayList<Wal.V> vList = new ArrayList<>();
        // merged cv list size must be small
        for (var one : mergedCvList) {
            var cv = one.cv;
            var key = one.key;
            var bucketIndex = one.bucketIndex;

            var calWalGroupIndex = Wal.calWalGroupIndex(bucketIndex);
            if (calWalGroupIndex != walGroupIndex) {
                continue;
            }

            vList.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
                    key, cv.encode(), true));
            if (!segmentIndexList.contains(one.segmentIndex)) {
                segmentIndexList.add(one.segmentIndex);
            }
        }

        if (vList.isEmpty()) {
            return null;
        }

        return new OneSlot.BeforePersistWalExt2FromMerge(segmentIndexList, vList);
    }

    void removeMergedButNotPersistedAfterPersistWal(ArrayList<Integer> segmentIndexList, int walGroupIndex) {
        mergedCvList.removeIf(one -> Wal.calWalGroupIndex(one.bucketIndex) == walGroupIndex);
        mergedSegmentSet.removeIf(one -> segmentIndexList.contains(one.segmentIndex));

        var doLog = Debug.getInstance().logMerge && logMergeCount % 1000 == 0;
        if (doLog) {
            log.info("After remove merged but not persisted, merged segment set: {}, merged cv list size: {}", mergedSegmentSet, mergedCvList.size());
        }
    }

    void persistAllMergedCvListInTargetSegmentIndexList(ArrayList<Integer> targetSegmentIndexList) {
        while (mergedSegmentSet.stream().anyMatch(one -> targetSegmentIndexList.contains(one.segmentIndex))) {
            persistFIFOMergedCvList();
        }
    }

    @SlaveNeedReplay
    void persistFIFOMergedCvList() {
        logMergeCount++;
        var doLog = Debug.getInstance().logMerge && logMergeCount % 1000 == 0;

        // once only persist firstly merged segments
        ArrayList<Integer> oncePersistSegmentIndexList = new ArrayList<>(MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST);
        // already sorted by segmentIndex
        for (var mergedSegment : mergedSegmentSet) {
            if (oncePersistSegmentIndexList.size() >= MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST) {
                break;
            }

            oncePersistSegmentIndexList.add(mergedSegment.segmentIndex);
        }

        var groupByWalGroupIndex = mergedCvList.stream()
                .filter(one -> oncePersistSegmentIndexList.contains(one.segmentIndex))
                .collect(Collectors.groupingBy(one -> Wal.calWalGroupIndex(one.bucketIndex)));

        if (groupByWalGroupIndex.size() > MERGED_SEGMENT_SIZE_THRESHOLD_ONCE_PERSIST) {
            log.warn("Go to persist merged cv list once, perf bad, group by wal group index size: {}", groupByWalGroupIndex.size());
        }

        XOneWalGroupPersist lastXForBinlog = null;
        int i = 0;
        for (var entry : groupByWalGroupIndex.entrySet()) {
            var walGroupIndex = entry.getKey();
            var subMergedCvList = entry.getValue();

            ArrayList<Wal.V> list = new ArrayList<>();
            for (var one : subMergedCvList) {
                var cv = one.cv;
                var key = one.key;
                var bucketIndex = one.bucketIndex;

                list.add(new Wal.V(cv.getSeq(), bucketIndex, cv.getKeyHash(), cv.getExpireAt(), cv.getDictSeqOrSpType(),
                        key, cv.encode(), true));
            }

            var xForBinlog = new XOneWalGroupPersist(false, false, walGroupIndex);
            // refer Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // list size is not large, need not multi batch persist
            oneSlot.chunk.persist(walGroupIndex, list, true, xForBinlog);

            i++;
            var isLastEntry = groupByWalGroupIndex.size() == i;

            if (!isLastEntry) {
                oneSlot.appendBinlog(xForBinlog);
            } else {
                lastXForBinlog = xForBinlog;
            }
        }

        if (doLog) {
            log.info("Compare chunk merged segment index end last time, end last time i: {}, ready to merged and persisted last i: {}",
                    oneSlot.chunk.mergedSegmentIndexEndLastTime, oncePersistSegmentIndexList.getLast());
        }

        var sb = new StringBuilder();
        var it = mergedSegmentSet.iterator();

        while (it.hasNext()) {
            var one = it.next();
            if (!oncePersistSegmentIndexList.contains(one.segmentIndex)) {
                continue;
            }

            // can reuse this chunk by segment segmentIndex
            oneSlot.updateSegmentMergeFlag(one.segmentIndex, Chunk.Flag.merged_and_persisted.flagByte, 0L);
            it.remove();
            sb.append(one.segmentIndex).append(";");

            // lastXForBinlog must not be null
            lastXForBinlog.putUpdatedChunkSegmentFlagWithSeq(one.segmentIndex, Chunk.Flag.merged_and_persisted.flagByte, 0L);
        }
        oneSlot.appendBinlog(lastXForBinlog);

        if (doLog) {
            log.info("P s:{}, {}", slot, sb);
        }

        mergedCvList.removeIf(one -> oncePersistSegmentIndexList.contains(one.segmentIndex));
    }

    boolean persistFIFOMergedCvListIfBatchSizeOk() {
        if (mergedCvList.size() < MERGED_CV_SIZE_THRESHOLD) {
            if (mergedSegmentSet.size() < MERGED_SEGMENT_SIZE_THRESHOLD) {
                return false;
            }
        }

        persistFIFOMergedCvList();
        return true;
    }

    long logMergeCount = 0;

    public ChunkMergeWorker(short slot, OneSlot oneSlot) {
        this.slot = slot;
        this.oneSlot = oneSlot;
    }

    @Override
    public long estimate() {
        long size = 0;
        size += RamUsageEstimator.sizeOfCollection(mergedCvList);
        size += RamUsageEstimator.sizeOfCollection(mergedSegmentSet);
        return size;
    }

    @Override
    public Map<String, Double> collect() {
        var map = new HashMap<String, Double>();

        if (mergedSegmentCount > 0) {
            map.put("merged_segment_count", (double) mergedSegmentCount);
            double mergedSegmentCostTAvg = (double) mergedSegmentCostTimeTotalUs / mergedSegmentCount;
            map.put("merged_segment_cost_time_avg_us", mergedSegmentCostTAvg);

            map.put("merged_valid_cv_count_total", (double) validCvCountTotal);
            map.put("merged_invalid_cv_count_total", (double) invalidCvCountTotal);

            double validCvCountAvg = (double) validCvCountTotal / mergedSegmentCount;
            map.put("merged_valid_cv_count_avg", validCvCountAvg);

            double validCvRate = (double) validCvCountTotal / (validCvCountTotal + invalidCvCountTotal);
            map.put("merged_valid_cv_ratio", validCvRate);
        }

        map.put("merged_last_merged_segment_index", (double) lastMergedSegmentIndex);
        map.put("merged_but_not_persisted_segment_count", (double) mergedSegmentSet.size());
        map.put("merged_but_not_persisted_cv_count", (double) mergedCvList.size());

        return map;
    }
}
