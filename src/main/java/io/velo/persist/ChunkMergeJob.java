package io.velo.persist;

import com.github.luben.zstd.Zstd;
import io.velo.*;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.incremental.XChunkSegmentFlagUpdate;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.stream.Collectors;

import static io.velo.persist.FdReadWrite.BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;

public class ChunkMergeJob {
    private final short slot;
    private final ArrayList<Integer> needMergeSegmentIndexList;
    private final ChunkMergeWorker chunkMergeWorker;
    private final OneSlot oneSlot;
    private final SnowFlake snowFlake;

    @VisibleForTesting
    int validCvCountAfterRun = 0;
    @VisibleForTesting
    int invalidCvCountAfterRun = 0;

    private static final Logger log = LoggerFactory.getLogger(ChunkMergeJob.class);

    public ChunkMergeJob(short slot, ArrayList<Integer> needMergeSegmentIndexList, ChunkMergeWorker chunkMergeWorker, SnowFlake snowFlake) {
        this.slot = slot;
        this.needMergeSegmentIndexList = needMergeSegmentIndexList;
        this.chunkMergeWorker = chunkMergeWorker;
        this.oneSlot = chunkMergeWorker.oneSlot;
        this.snowFlake = snowFlake;
    }

    public int run() {
        // recycle, need spit to two part
        if (needMergeSegmentIndexList.getLast() - needMergeSegmentIndexList.getFirst() > oneSlot.chunk.halfSegmentNumber) {
            if (!needMergeSegmentIndexList.contains(0)) {
                throw new IllegalStateException("Recycle merge chunk, s=" + slot + ", segment index need include 0, but: " + needMergeSegmentIndexList);
            }

            var onePart = new ArrayList<Integer>();
            var anotherPart = new ArrayList<Integer>();
            for (var segmentIndex : needMergeSegmentIndexList) {
                if (segmentIndex < oneSlot.chunk.halfSegmentNumber) {
                    onePart.add(segmentIndex);
                } else {
                    anotherPart.add(segmentIndex);
                }
            }
            log.warn("Recycle merge chunk, s={}, one part need merge segment index list ={}, another part need merge segment index list={}",
                    slot, onePart, anotherPart);

            var job1 = new ChunkMergeJob(slot, onePart, chunkMergeWorker, snowFlake);
            var job2 = new ChunkMergeJob(slot, anotherPart, chunkMergeWorker, snowFlake);

            // first do job2, then do job1
            var validCvCount2 = job2.run();
            var validCvCount1 = job1.run();
            return validCvCount1 + validCvCount2;
        }

        var batchCount = needMergeSegmentIndexList.size() / BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;
        if (needMergeSegmentIndexList.size() % BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE != 0) {
            batchCount++;
        }
        try {
            for (int i = 0; i < batchCount; i++) {
                var subList = needMergeSegmentIndexList.subList(i * BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE,
                        Math.min((i + 1) * BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE, needMergeSegmentIndexList.size()));
                mergeSegments(subList);
            }
            return validCvCountAfterRun;
        } catch (Exception e) {
            log.error("Merge chunk error, s={}, i={}, message={}", slot, needMergeSegmentIndexList, e.getMessage());
            log.error("Merge chunk error", e);
            return -1;
        }
    }

    public static class CvWithKeyAndSegmentOffset {
        public CvWithKeyAndSegmentOffset(CompressedValue cv, String key, int segmentOffset, int segmentIndex, byte subBlockIndex) {
            this.cv = cv;
            this.key = key;
            this.segmentOffset = segmentOffset;
            this.segmentIndex = segmentIndex;
            this.subBlockIndex = subBlockIndex;
        }

        public CompressedValue cv;
        public final String key;
        final int segmentOffset;
        final int segmentIndex;
        final byte subBlockIndex;

        // calc for batch pread key buckets, for performance
        int bucketIndex;
        int walGroupIndex;

        public String shortString() {
            return "k=" + key + ", s=" + segmentIndex + ", sbi=" + subBlockIndex + ", so=" + segmentOffset;
        }

        @Override
        public String toString() {
            return "CvWithKeyAndSegmentOffset{" +
                    "key='" + key + '\'' +
                    ", segmentOffset=" + segmentOffset +
                    ", segmentIndex=" + segmentIndex +
                    ", subBlockIndex=" + subBlockIndex +
                    '}';
        }
    }

    private static class ValidCvCountRecord {
        ValidCvCountRecord(int segmentIndex) {
            this.segmentIndex = segmentIndex;
        }

        int validCvCount;
        int invalidCvCount;
        final int segmentIndex;
    }

    @SlaveNeedReplay
    @VisibleForTesting
    void mergeSegments(List<Integer> needMergeSegmentIndexList) {
        chunkMergeWorker.logMergeCount++;
        var doLog = Debug.getInstance().logMerge && chunkMergeWorker.logMergeCount % 1000 == 0;

        // unbox to use ==, not equals
        int firstSegmentIndex = needMergeSegmentIndexList.getFirst();
        var lastSegmentIndex = needMergeSegmentIndexList.getLast();
        if (needMergeSegmentIndexList.size() != lastSegmentIndex - firstSegmentIndex + 1) {
            throw new IllegalStateException("Merge segments index need be continuous, but: " + needMergeSegmentIndexList);
        }

        HashSet<Integer> skipSegmentIndexSet = new HashSet<>();
        for (var segmentIndex : needMergeSegmentIndexList) {
            var segmentFlag = oneSlot.getSegmentMergeFlag(segmentIndex);
            var flag = segmentFlag.flag();

            // not write yet, skip
            if (flag == Chunk.Flag.init) {
                skipSegmentIndexSet.add(segmentIndex);
                continue;
            }

            if (flag == Chunk.Flag.merged_and_persisted) {
                skipSegmentIndexSet.add(segmentIndex);
//                continue;
            }
        }

        chunkMergeWorker.lastMergedSegmentIndex = lastSegmentIndex;

        var allSkipped = needMergeSegmentIndexList.size() == skipSegmentIndexSet.size();
        if (allSkipped) {
            return;
        }

        chunkMergeWorker.mergedSegmentCount++;

        ArrayList<ValidCvCountRecord> validCvCountRecordList = new ArrayList<>(needMergeSegmentIndexList.size());
        for (var segmentIndex : needMergeSegmentIndexList) {
            validCvCountRecordList.add(new ValidCvCountRecord(segmentIndex));
        }

        // read all segments to memory, then compare with key buckets
        int chunkSegmentLength = ConfForSlot.global.confChunk.segmentLength;
        var tmpCapacity = chunkSegmentLength / ConfForGlobal.estimateOneValueLength * BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE;
        ArrayList<CvWithKeyAndSegmentOffset> cvList = new ArrayList<>(tmpCapacity);

        var beginT = System.nanoTime();
        var segmentBytesBatchRead = oneSlot.preadForMerge(firstSegmentIndex, BATCH_ONCE_SEGMENT_COUNT_FOR_MERGE);

        boolean alreadyDoLog = false;
        int i = 0;
        for (var segmentIndex : needMergeSegmentIndexList) {
            if (skipSegmentIndexSet.contains(segmentIndex)) {
                // move to next segment
                i++;
                continue;
            }

            int relativeOffsetInBatchBytes = i * chunkSegmentLength;
            // refer to Chunk.ONCE_PREPARE_SEGMENT_COUNT
            // last segments not write at all, need skip
            if (segmentBytesBatchRead == null || relativeOffsetInBatchBytes >= segmentBytesBatchRead.length) {
                oneSlot.updateSegmentMergeFlag(segmentIndex, Chunk.Flag.merged_and_persisted, 0L);
                if (doLog) {
                    log.info("Set segment flag to persisted as not write at all, s={}, i={}", slot, segmentIndex);
                }
                i++;
                continue;
            }

            oneSlot.updateSegmentMergeFlag(segmentIndex, Chunk.Flag.merging, snowFlake.nextId());
            if (doLog && !alreadyDoLog) {
                log.info("Set segment flag to merging, s={}, i={}", slot, segmentIndex);
                alreadyDoLog = true;
            }

            var expiredCount = readToCvList(cvList, segmentBytesBatchRead, relativeOffsetInBatchBytes, chunkSegmentLength, segmentIndex, oneSlot);
            if (expiredCount > 0) {
                var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);
                validCvCountRecord.invalidCvCount += expiredCount;
            }
            i++;
        }

//        assert oneSlot.keyLoader != null;
        // calc bucket index and wal group index
        for (var one : cvList) {
            one.bucketIndex = KeyHash.bucketIndex(one.cv.getKeyHash(), ConfForSlot.global.confBucket.bucketsPerSlot);
            one.walGroupIndex = Wal.calWalGroupIndex(one.bucketIndex);
        }
        // compare to current wal or persist value meta in key buckets, remove those deleted or expired
        removeOld(oneSlot, cvList, validCvCountRecordList);

        for (var validCvCountRecord : validCvCountRecordList) {
            validCvCountAfterRun += validCvCountRecord.validCvCount;
            invalidCvCountAfterRun += validCvCountRecord.invalidCvCount;
        }

        HashSet<Integer> hasValidCvSegmentIndexSet = new HashSet<>();
        for (var one : cvList) {
            // use memory list, and threshold, then persist to chunk
            chunkMergeWorker.addMergedCv(new ChunkMergeWorker.CvWithKeyAndBucketIndexAndSegmentIndex(one.cv, one.key, one.bucketIndex, one.segmentIndex));
            hasValidCvSegmentIndexSet.add(one.segmentIndex);
        }

        var xChunkSegmentFlagUpdate = new XChunkSegmentFlagUpdate();
        for (var segmentIndex : needMergeSegmentIndexList) {
            var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);

            if (hasValidCvSegmentIndexSet.contains(segmentIndex)) {
                var segmentSeq = snowFlake.nextId();
                oneSlot.updateSegmentMergeFlag(segmentIndex, Chunk.Flag.merged, segmentSeq);
                xChunkSegmentFlagUpdate.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Chunk.Flag.merged, segmentSeq);

                if (doLog) {
                    log.info("Set segment flag to merged, s={}, i={}, valid cv count={}, invalid cv count={}",
                            slot, segmentIndex, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                }

                chunkMergeWorker.addMergedSegment(segmentIndex, validCvCountRecord.validCvCount);
            } else {
                oneSlot.updateSegmentMergeFlag(segmentIndex, Chunk.Flag.merged_and_persisted, 0L);
                xChunkSegmentFlagUpdate.putUpdatedChunkSegmentFlagWithSeq(segmentIndex, Chunk.Flag.merged_and_persisted, 0L);

                if (doLog && segmentIndex == firstSegmentIndex) {
                    log.info("Set segment flag to persisted, s={}, i={}, valid cv count={}, invalid cv count={}",
                            slot, segmentIndex, validCvCountRecord.validCvCount, validCvCountRecord.invalidCvCount);
                }
            }
        }

        if (!xChunkSegmentFlagUpdate.isEmpty()) {
            oneSlot.appendBinlog(xChunkSegmentFlagUpdate);
        }

        chunkMergeWorker.persistFIFOMergedCvListIfBatchSizeOk();

        var costT = (System.nanoTime() - beginT) / 1000;
        chunkMergeWorker.mergedSegmentCostTimeTotalUs += costT;

        chunkMergeWorker.validCvCountTotal += validCvCountAfterRun;
        chunkMergeWorker.invalidCvCountTotal += invalidCvCountAfterRun;
    }

    // return expired count
    static int readToCvList(ArrayList<CvWithKeyAndSegmentOffset> cvList, byte[] segmentBytesBatchRead, int relativeOffsetInBatchBytes,
                            int chunkSegmentLength, int segmentIndex, OneSlot oneSlot) {
        final int[] expiredCountArray = {0};
        var buffer = ByteBuffer.wrap(segmentBytesBatchRead, relativeOffsetInBatchBytes, Math.min(segmentBytesBatchRead.length, chunkSegmentLength)).slice();
        // sub blocks
        // refer to SegmentBatch tight HEADER_LENGTH
        for (int subBlockIndex = 0; subBlockIndex < SegmentBatch.MAX_BLOCK_NUMBER; subBlockIndex++) {
            // position to target sub block
            buffer.position(SegmentBatch.subBlockMetaPosition(subBlockIndex));
            var subBlockOffset = buffer.getShort();
            if (subBlockOffset == 0) {
                break;
            }
            var subBlockLength = buffer.getShort();

            var decompressedBytes = new byte[chunkSegmentLength];
            var d = Zstd.decompressByteArray(decompressedBytes, 0, chunkSegmentLength,
                    segmentBytesBatchRead, relativeOffsetInBatchBytes + subBlockOffset, subBlockLength);
            if (d != chunkSegmentLength) {
                throw new IllegalStateException("Decompress error, s=" + oneSlot.slot()
                        + ", i=" + segmentIndex + ", sbi=" + subBlockIndex + ", d=" + d + ", chunkSegmentLength=" + chunkSegmentLength);
            }

            int finalSubBlockIndex = subBlockIndex;
            SegmentBatch.iterateFromSegmentBytes(decompressedBytes, (key, cv, offsetInThisSegment) -> {
                // exclude expired
                if (cv.isExpired()) {
                    expiredCountArray[0]++;
                    return;
                }
                cvList.add(new CvWithKeyAndSegmentOffset(cv, key, offsetInThisSegment, segmentIndex, (byte) finalSubBlockIndex));
            });
        }
        return expiredCountArray[0];
    }

    private void removeOld(OneSlot oneSlot, ArrayList<CvWithKeyAndSegmentOffset> cvList, ArrayList<ValidCvCountRecord> validCvCountRecordList) {
        var dictMap = DictMap.getInstance();
        var firstSegmentIndex = validCvCountRecordList.get(0).segmentIndex;
        ArrayList<CvWithKeyAndSegmentOffset> toRemoveCvList = new ArrayList<>(cvList.size());

        var groupByWalGroupIndex = cvList.stream().collect(Collectors.groupingBy(one -> one.walGroupIndex));
        for (var entry : groupByWalGroupIndex.entrySet()) {
            var walGroupIndex = entry.getKey();
            var keyBucketsInOneWalGroup = new KeyBucketsInOneWalGroup(slot, walGroupIndex, oneSlot.keyLoader);

            var cvListInOneWalGroup = entry.getValue();
            for (var one : cvListInOneWalGroup) {
                var key = one.key;
                var cv = one.cv;
                var segmentOffset = one.segmentOffset;
                var segmentIndex = one.segmentIndex;
                var subBlockIndex = one.subBlockIndex;
                var bucketIndex = one.bucketIndex;

                var validCvCountRecord = validCvCountRecordList.get(segmentIndex - firstSegmentIndex);
                // already excluded expired
//                if (cv.isExpired()) {
//                    validCvCountRecord.invalidCvCount++;
//                    toRemoveCvList.add(one);
//                    continue;
//                }

                byte[] valueBytesCurrent;
                var tmpWalValueBytes = oneSlot.getFromWal(key, bucketIndex);
                if (tmpWalValueBytes != null) {
                    // from wal, is the newest
                    // if deleted, discard
                    if (CompressedValue.isDeleted(tmpWalValueBytes)) {
                        valueBytesCurrent = null;
                    } else {
                        valueBytesCurrent = tmpWalValueBytes;
                    }
                } else {
                    // from key buckets
                    var valueBytesWithExpireAtAndSeq = keyBucketsInOneWalGroup.getValue(bucketIndex, key.getBytes(), cv.getKeyHash());
                    if (valueBytesWithExpireAtAndSeq == null || valueBytesWithExpireAtAndSeq.isExpired()) {
                        valueBytesCurrent = null;
                    } else {
                        if (valueBytesWithExpireAtAndSeq.seq() != cv.getSeq()) {
                            valueBytesCurrent = null;
                        } else {
                            valueBytesCurrent = valueBytesWithExpireAtAndSeq.valueBytes();
                        }
                    }
                }
                if (valueBytesCurrent == null) {
                    validCvCountRecord.invalidCvCount++;
                    toRemoveCvList.add(one);
                    continue;
                }

                // if not meta, short value encoded or from wal cv encoded
                if (!PersistValueMeta.isPvm(valueBytesCurrent)) {
                    // compare seq
                    var buffer = ByteBuffer.wrap(valueBytesCurrent);

                    long valueSeqCurrent;
                    // refer to CompressedValue decode
                    var firstByte = buffer.get(0);
                    if (firstByte < 0) {
                        valueSeqCurrent = buffer.position(1).getLong();
                    } else {
                        // normal compressed value encoded
                        valueSeqCurrent = buffer.getLong();
                    }

                    if (cv.getSeq() < valueSeqCurrent) {
                        // cv is old， discard
                        validCvCountRecord.invalidCvCount++;
                        toRemoveCvList.add(one);
                    } else {
                        // this else block will never happen
                        // because compressed value is newest from write batch kv
                        // from key loader, must be persisted value meta
                        throw new IllegalStateException("Merge compressed value is newer than write batch compressed value, s=" + slot +
                                ", i=" + segmentIndex + ", key=" + key + ", merge cv seq=" + cv.getSeq() +
                                ", write batch cv seq=" + valueSeqCurrent);
                    }
                } else {
                    var pvmCurrent = PersistValueMeta.decode(valueBytesCurrent);
                    if (!pvmCurrent.isTargetSegment(segmentIndex, subBlockIndex, segmentOffset)) {
                        // cv is old， discard
                        validCvCountRecord.invalidCvCount++;
                        toRemoveCvList.add(one);
                    } else {
                        validCvCountRecord.validCvCount++;

                        // if there is a new dict, compress use new dict and replace if compressed length is smaller
                        if (cv.isUseDict() && cv.getDictSeqOrSpType() == Dict.SELF_ZSTD_DICT_SEQ) {
                            var dict = dictMap.getDict(TrainSampleJob.keyPrefixOrSuffixGroup(key));
                            if (dict != null) {
                                var rawBytes = cv.decompress(Dict.SELF_ZSTD_DICT);
                                var newCompressedCv = CompressedValue.compress(rawBytes, dict);
                                if (!newCompressedCv.isIgnoreCompression(rawBytes) && newCompressedCv.getCompressedLength() < cv.getCompressedLength()) {
                                    // replace
                                    newCompressedCv.setSeq(cv.getSeq());
                                    newCompressedCv.setDictSeqOrSpType(dict.getSeq());
                                    newCompressedCv.setKeyHash(cv.getKeyHash());
                                    newCompressedCv.setExpireAt(cv.getExpireAt());

                                    one.cv = newCompressedCv;
                                }
                            }
                        }
                    }
                }
            }
        }

        cvList.removeAll(toRemoveCvList);
    }
}
