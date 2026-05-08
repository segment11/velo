package io.velo.persist;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;

import io.velo.KeyHash;
import org.jetbrains.annotations.NotNull;

/**
 * Rebuilds one WAL group's key-bucket index from chunk segments.
 */
public class ChunkWalGroupRebuilder {
    public enum Mode {
        PREVIEW("preview", false),
        APPLY("apply", true);

        private final String text;
        private final boolean apply;

        Mode(String text, boolean apply) {
            this.text = text;
            this.apply = apply;
        }

        public String text() {
            return text;
        }

        boolean isApply() {
            return apply;
        }

        public static Mode fromText(String text) {
            for (var mode : values()) {
                if (mode.text.equalsIgnoreCase(text)) {
                    return mode;
                }
            }
            return null;
        }
    }

    public record RebuildResult(short slot,
                                int walGroupIndex,
                                String mode,
                                int segmentsScanned,
                                int segmentsInvalid,
                                int recordsDecoded,
                                int recordsExpired,
                                int recordsWalGroupMismatch,
                                int recordsAfterSeqMerge,
                                int staleRecords,
                                int bucketsTouched,
                                boolean applied) {
    }

    private record LatestRecord(String key, long keyHash, PersistValueMeta pvm) {
    }

    private final short slot;
    private final OneSlot oneSlot;

    ChunkWalGroupRebuilder(short slot, @NotNull OneSlot oneSlot) {
        this.slot = slot;
        this.oneSlot = oneSlot;
    }

    RebuildResult rebuild(int walGroupIndex, @NotNull Mode mode) {
        if (walGroupIndex < 0 || walGroupIndex >= Wal.calcWalGroupNumber()) {
            throw new IllegalArgumentException("Wal group index out of range, slot=" + slot + ", wal group index=" + walGroupIndex);
        }

        var latestByKey = new HashMap<String, LatestRecord>();
        var touchedBucketIndexes = new HashSet<Integer>();
        var stats = new ScanStats();

        oneSlot.metaChunkSegmentFlagSeq.iterateAll((segmentIndex, flagByte, segmentSeq, segmentWalGroupIndex) -> {
            if (flagByte != Chunk.SEGMENT_FLAG_HAS_DATA || segmentWalGroupIndex != walGroupIndex) {
                return true;
            }

            stats.segmentsScanned++;
            try {
                var segmentBytes = oneSlot.chunk.readOneSegment(segmentIndex);
                if (segmentBytes == null) {
                    stats.segmentsInvalid++;
                    return true;
                }

                var cvList = new ArrayList<SegmentBatch2.CvWithKeyAndSegmentOffset>();
                var expiredCount = SegmentBatch2.readToCvList(cvList, segmentBytes, 0,
                        oneSlot.chunk.chunkSegmentLength, segmentIndex, slot);
                stats.recordsExpired += expiredCount;
                stats.recordsDecoded += cvList.size() + expiredCount;

                for (var one : cvList) {
                    var cv = one.cv();
                    var bucketIndex = KeyHash.bucketIndex(cv.getKeyHash());
                    var cvWalGroupIndex = Wal.calcWalGroupIndex(bucketIndex);
                    if (cvWalGroupIndex != walGroupIndex) {
                        stats.recordsWalGroupMismatch++;
                        continue;
                    }

                    var pvm = new PersistValueMeta();
                    pvm.shortType = KeyLoader.transferToShortType(cv.getDictSeqOrSpType());
                    pvm.subBlockIndex = one.subBlockIndex();
                    pvm.segmentIndex = one.segmentIndex();
                    pvm.segmentOffset = one.segmentOffset();
                    pvm.expireAt = cv.getExpireAt();
                    pvm.seq = cv.getSeq();
                    pvm.key = one.key();
                    pvm.keyHash = cv.getKeyHash();
                    pvm.bucketIndex = bucketIndex;

                    var mapKey = one.key() + '\u0000' + cv.getKeyHash();
                    var old = latestByKey.get(mapKey);
                    if (old == null) {
                        latestByKey.put(mapKey, new LatestRecord(one.key(), cv.getKeyHash(), pvm));
                        touchedBucketIndexes.add(bucketIndex);
                    } else if (pvm.seq > old.pvm.seq) {
                        stats.staleRecords++;
                        latestByKey.put(mapKey, new LatestRecord(one.key(), cv.getKeyHash(), pvm));
                        touchedBucketIndexes.add(bucketIndex);
                    } else {
                        stats.staleRecords++;
                    }
                }
            } catch (Exception e) {
                stats.segmentsInvalid++;
            }
            return true;
        });

        var pvmList = new ArrayList<PersistValueMeta>(latestByKey.size());
        for (var one : latestByKey.values()) {
            pvmList.add(one.pvm);
        }
        pvmList.sort(java.util.Comparator.comparingInt(pvm -> pvm.bucketIndex));

        if (mode.isApply()) {
            assert oneSlot.keyLoader != null;
            oneSlot.keyLoader.replacePvmListBatchInOneWalGroupForRebuild(walGroupIndex, pvmList);
        }

        return new RebuildResult(slot, walGroupIndex, mode.text(),
                stats.segmentsScanned, stats.segmentsInvalid, stats.recordsDecoded, stats.recordsExpired,
                stats.recordsWalGroupMismatch, pvmList.size(), stats.staleRecords, touchedBucketIndexes.size(), mode.isApply());
    }

    private static class ScanStats {
        private int segmentsScanned;
        private int segmentsInvalid;
        private int recordsDecoded;
        private int recordsExpired;
        private int recordsWalGroupMismatch;
        private int staleRecords;
    }
}
