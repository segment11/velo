package io.velo.persist.index;

import io.velo.NeedCleanUp;
import io.velo.SnowFlake;
import io.velo.repl.MasterReset;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.HashMap;
import java.util.TreeMap;
import java.util.TreeSet;

public class ReverseIndexChunk implements NeedCleanUp {
    // seq is long 8 bytes with time millis, 256KB = 256 * 1024 / 8 = 32768
    // means one word hold 32768 reverse document id
    // if one word hold more documents, need split by time range or other strategy
    // eg: 5min one MetaIndexWords, one seconds one word can contain: 32768 / 5 / 60 = 109
    private static final int ONE_WORD_HOLD_ONE_SEGMENT_LENGTH_KB = 256;
    @VisibleForTesting
    static final int ONE_WORD_HOLD_ONE_SEGMENT_LENGTH = ONE_WORD_HOLD_ONE_SEGMENT_LENGTH_KB * 1024;
    private static final int ONE_WORD_HOLD_ONE_SEGMENT_LONG_ID_COUNT = ONE_WORD_HOLD_ONE_SEGMENT_LENGTH / 8;

    // reuse as thread safe
    private final byte[] oneSegmentBytesForRead = new byte[ONE_WORD_HOLD_ONE_SEGMENT_LENGTH];
    private final ByteBuffer byteBufferForRead = ByteBuffer.wrap(oneSegmentBytesForRead);

    // 2GB / 256K = 8192
    // one file max 2GB, 8192 * 256KB = 2GB, one segment for delta update, so 4096 words need one file
    // one segment contains 32768 long, 4096 * 32768 = 134217728
    static final int ONE_FD_MAX_HOLD_WORD_COUNT = 4096;
    static final int ONE_FD_ESTIMATE_KV_COUNT = 4096 * 32768;

    private static final String CHUNK_FILE_NAME_PREFIX = "index-chunk-";

    // use 8 segments save meta info, 8 * 256KB = 2MB
    @VisibleForTesting
    static final int HEADER_USED_SEGMENT_COUNT = 8;
    // when meta index words clear, all fill byte 0, segment index = 0 will never be used
    private static final int HEADER_FOR_META_LENGTH = HEADER_USED_SEGMENT_COUNT * ONE_WORD_HOLD_ONE_SEGMENT_LENGTH_KB * 1024;

    // 4 bytes int for segment index, 4 bytes int for write index, 4 bytes short for word length, 32 bytes for word
    // one fd = 8192 words, 8192 * (4 + 4 + 4 + 32) = 8192 * 44 = 360448, max 4 fd = need 1.4MB
    private static final int ONE_SEGMENT_INDEX_META_LENGTH = 4 + 4 + 4 + 32;

    @VisibleForTesting
    // 160KB * 8 = 1.28MB > 1MB
    static final int MAX_FD_PER_CHUNK = 4;

    private final byte workerId;

    @VisibleForTesting
    final int segmentNumberPerFd;
    @VisibleForTesting
    final int maxSegmentNumber;

    private final RandomAccessFile[] rafArray;

    private final byte[] metaBytes = new byte[HEADER_FOR_META_LENGTH];
    private final ByteBuffer metaByteBuffer = ByteBuffer.wrap(metaBytes);

    // for repl
    byte[] readOneSegment(int segmentIndex) {
        var targetFdIndex = targetFdIndex(segmentIndex);
        var targetSegmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);

        try {
            var raf = rafArray[targetFdIndex];
            if (raf.length() < ONE_WORD_HOLD_ONE_SEGMENT_LENGTH) {
                return new byte[1];
            }

            var bytes = new byte[ONE_WORD_HOLD_ONE_SEGMENT_LENGTH];
            var targetSegmentOffsetInRaf = (long) targetSegmentIndexTargetFd * ONE_WORD_HOLD_ONE_SEGMENT_LENGTH;
            raf.seek(targetSegmentOffsetInRaf);
            var n = raf.read(bytes);
            if (n != ONE_WORD_HOLD_ONE_SEGMENT_LENGTH) {
                throw new IllegalStateException("Index read by segment index error, expect: " + ONE_WORD_HOLD_ONE_SEGMENT_LENGTH + ", n: " + n + ", worker id: " + workerId);
            }
            return bytes;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    void writeOneSegment(int segmentIndex, byte[] bytes) {
        if (segmentIndex >= maxSegmentNumber) {
            throw new IllegalArgumentException("Segment index out of range: " + segmentIndex);
        }

        var targetFdIndex = targetFdIndex(segmentIndex);
        var targetSegmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);

        var raf = rafArray[targetFdIndex];
        try {
            var targetSegmentOffsetInRaf = (long) targetSegmentIndexTargetFd * ONE_WORD_HOLD_ONE_SEGMENT_LENGTH;
            raf.seek(targetSegmentOffsetInRaf);
            raf.write(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final int expiredIfSecondsFromNow;

    private static final Logger log = LoggerFactory.getLogger(ReverseIndexChunk.class);

    public ReverseIndexChunk(byte workerId, File workerIdDir, byte fdPerChunk, int expiredIfSecondsFromNow) throws IOException {
        if (fdPerChunk > MAX_FD_PER_CHUNK) {
            throw new IllegalArgumentException("Reverse index chunk init, fd per chunk must less than " + MAX_FD_PER_CHUNK);
        }

        this.workerId = workerId;

        // 2GB / 256KB = 8192
        this.segmentNumberPerFd = 2048 * 1024 / ONE_WORD_HOLD_ONE_SEGMENT_LENGTH_KB;
        this.maxSegmentNumber = segmentNumberPerFd * fdPerChunk;

        this.expiredIfSecondsFromNow = expiredIfSecondsFromNow;

        this.rafArray = new RandomAccessFile[fdPerChunk];
        for (int i = 0; i < fdPerChunk; i++) {
            var chunkFile = new File(workerIdDir, CHUNK_FILE_NAME_PREFIX + i);
            if (!chunkFile.exists()) {
                FileUtils.touch(chunkFile);
            }
            var raf = new RandomAccessFile(chunkFile, "rw");
            rafArray[i] = raf;
        }

        iterateMeta();
    }

    // lower case word
    @VisibleForTesting
    final TreeMap<Integer, String> segmentIndexToWord = new TreeMap<>();
    @VisibleForTesting
    final TreeMap<String, Integer> wordToSegmentIndex = new TreeMap<>();

    @TestOnly
    void setMinLength(int minLength) {
        for (int i = 0; i < rafArray.length; i++) {
            var raf = rafArray[i];
            try {
                if (raf.length() >= minLength) {
                    continue;
                }
                raf.setLength(minLength);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @MasterReset
    void iterateMeta() throws IOException {
        var firstRaf = rafArray[0];
        if (firstRaf.length() > HEADER_FOR_META_LENGTH) {
            // read meta info
            firstRaf.seek(0);
            firstRaf.read(metaBytes);
        }

        for (int i = HEADER_USED_SEGMENT_COUNT; i < maxSegmentNumber; i++) {
            var metaOffset = i * ONE_SEGMENT_INDEX_META_LENGTH;

            metaByteBuffer.position(metaOffset);
            int segmentIndex = metaByteBuffer.getInt();
            if (segmentIndex == 0) {
                continue;
            }

            var writeIndex = metaByteBuffer.getInt();
            writeIndexBySegmentIndexCached.put(segmentIndex, writeIndex);

            var wordLength = metaByteBuffer.getInt();
            byte[] wordBytes = new byte[wordLength];
            metaByteBuffer.get(wordBytes);

            var lowerCaseWord = new String(wordBytes);
            segmentIndexToWord.put(segmentIndex, lowerCaseWord);
            wordToSegmentIndex.put(lowerCaseWord, segmentIndex);
        }
    }

    private int findOneSegmentAvailableForOneWord(String lowerCaseWord) {
        for (int i = HEADER_USED_SEGMENT_COUNT; i < maxSegmentNumber; i++) {
            var usedByWord = segmentIndexToWord.get(i);
            if (usedByWord == null) {
                return i;
            }

            if (usedByWord.equals(lowerCaseWord)) {
                return i;
            }
        }
        return -1;
    }

    int initMetaForOneWord(String lowerCaseWord) {
        var alreadyInitSegmentIndex = wordToSegmentIndex.get(lowerCaseWord);
        if (alreadyInitSegmentIndex != null) {
            return alreadyInitSegmentIndex;
        }

        var segmentIndex = findOneSegmentAvailableForOneWord(lowerCaseWord);
        if (segmentIndex == -1) {
            throw new RuntimeException("No available segment for word: " + lowerCaseWord);
        }

        var metaOffset = segmentIndex * ONE_SEGMENT_INDEX_META_LENGTH;
        metaByteBuffer.position(metaOffset);
        metaByteBuffer.putInt(segmentIndex);
        metaByteBuffer.putInt(0);
        metaByteBuffer.putInt(lowerCaseWord.length());
        metaByteBuffer.put(lowerCaseWord.getBytes());

        var changedMetaBytes = new byte[ONE_SEGMENT_INDEX_META_LENGTH];
        metaByteBuffer.position(metaOffset).get(changedMetaBytes);

        var firstRaf = rafArray[0];
        try {
            firstRaf.seek(metaOffset);
            firstRaf.write(changedMetaBytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        segmentIndexToWord.put(segmentIndex, lowerCaseWord);
        wordToSegmentIndex.put(lowerCaseWord, segmentIndex);

        return segmentIndex;
    }

    @VisibleForTesting
    int targetFdIndex(int targetSegmentIndex) {
        return targetSegmentIndex / segmentNumberPerFd;
    }

    @VisibleForTesting
    int targetSegmentIndexTargetFd(int targetSegmentIndex) {
        return targetSegmentIndex % segmentNumberPerFd;
    }

    private final HashMap<Integer, Integer> writeIndexBySegmentIndexCached = new HashMap<>();

    private void updateMetaWriteIndexBySegmentIndex(int segmentIndex, int writeIndex) {
        var metaOffset = segmentIndex * ONE_SEGMENT_INDEX_META_LENGTH;
        metaByteBuffer.putInt(metaOffset + 4, writeIndex);

        var firstRaf = rafArray[0];
        try {
            firstRaf.seek(metaOffset + 4);
            firstRaf.writeInt(writeIndex);

            writeIndexBySegmentIndexCached.put(segmentIndex, writeIndex);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @VisibleForTesting
    void merge(int segmentIndex, long longId) {
        var targetFdIndex = targetFdIndex(segmentIndex);
        var targetSegmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);
        var targetSegmentOffsetInRaf = (long) targetSegmentIndexTargetFd * ONE_WORD_HOLD_ONE_SEGMENT_LENGTH;

        var currentTimeStamp = System.currentTimeMillis();

        var raf = rafArray[targetFdIndex];
        try {
            raf.seek(targetSegmentOffsetInRaf);
            // n should be oneSegmentBytesForRead.length
            var n = raf.read(oneSegmentBytesForRead);

            TreeSet<Long> set = new TreeSet<>();

            byteBufferForRead.position(0);
            byteBufferForRead.limit(n);
            // vector optimize, todo
            for (int i = 0; i < ONE_WORD_HOLD_ONE_SEGMENT_LONG_ID_COUNT; i++) {
                long existLongId = byteBufferForRead.getLong(i * 8);
                if (SnowFlake.isExpired(existLongId, expiredIfSecondsFromNow, currentTimeStamp)) {
                    continue;
                }
                set.add(existLongId);
            }
            if (set.size() >= ONE_WORD_HOLD_ONE_SEGMENT_LONG_ID_COUNT) {
                throw new RuntimeException("One word hold one segment long id count reach max: " + ONE_WORD_HOLD_ONE_SEGMENT_LONG_ID_COUNT);
            }
            set.add(longId);

            Arrays.fill(oneSegmentBytesForRead, (byte) 0);
            byteBufferForRead.clear();
            for (var longIdOne : set) {
                byteBufferForRead.putLong(longIdOne);
            }

            raf.seek(targetSegmentOffsetInRaf);
            raf.write(oneSegmentBytesForRead);

            updateMetaWriteIndexBySegmentIndex(segmentIndex, set.size() * 8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    int addLongId(String lowerCaseWord, long longId) {
        var segmentIndex = wordToSegmentIndex.get(lowerCaseWord);
        if (segmentIndex == null) {
            throw new RuntimeException("No segment index for word: " + lowerCaseWord);
        }

        var writeIndex = writeIndexBySegmentIndexCached.get(segmentIndex);
        if (writeIndex == null) {
            writeIndex = 0;
        }

        if (writeIndex == ONE_WORD_HOLD_ONE_SEGMENT_LENGTH) {
            // need merge
            merge(segmentIndex, longId);
        } else {
            var targetFdIndex = targetFdIndex(segmentIndex);
            var targetSegmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);
            var targetSegmentOffsetInRaf = (long) targetSegmentIndexTargetFd * ONE_WORD_HOLD_ONE_SEGMENT_LENGTH;

            var raf = rafArray[targetFdIndex];
            try {
                raf.seek(targetSegmentOffsetInRaf + writeIndex);
                raf.writeLong(longId);

                updateMetaWriteIndexBySegmentIndex(segmentIndex, writeIndex + 8);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
        return segmentIndex;
    }

    TreeSet<Long> getLongIds(String lowerCaseWord, int skipCount, int limit) {
        var segmentIndex = wordToSegmentIndex.get(lowerCaseWord);
        if (segmentIndex == null) {
            return new TreeSet<>();
        }

        var writeIndex = writeIndexBySegmentIndexCached.get(segmentIndex);
        if (writeIndex == null || writeIndex == 0) {
            return new TreeSet<>();
        }

        var targetFdIndex = targetFdIndex(segmentIndex);
        var targetSegmentIndexTargetFd = targetSegmentIndexTargetFd(segmentIndex);
        var targetSegmentOffsetInRaf = (long) targetSegmentIndexTargetFd * ONE_WORD_HOLD_ONE_SEGMENT_LENGTH;

        var currentTimeStamp = System.currentTimeMillis();

        var raf = rafArray[targetFdIndex];
        try {
            raf.seek(targetSegmentOffsetInRaf);
            var n = raf.read(oneSegmentBytesForRead, 0, writeIndex);
            if (n != writeIndex) {
                throw new IllegalStateException("Index read by write index error, expect: " + writeIndex + ", n: " + n + ", worker id: " + workerId);
            }

            byteBufferForRead.position(0);
            byteBufferForRead.limit(writeIndex);

            TreeSet<Long> set = new TreeSet<>();

            for (int i = 0; i < writeIndex / 8; i++) {
                long existLongId = byteBufferForRead.getLong(i * 8);
                if (existLongId == 0L) {
                    break;
                }

                if (SnowFlake.isExpired(existLongId, expiredIfSecondsFromNow, currentTimeStamp)) {
                    continue;
                }

                if (skipCount > 0) {
                    skipCount--;
                    continue;
                }

                set.add(existLongId);
                if (set.size() >= limit) {
                    break;
                }
            }
            return set;
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    public void clear() {
        for (int i = 0; i < rafArray.length; i++) {
            var raf = rafArray[i];
            try {
                raf.seek(0);
                raf.write(new byte[HEADER_FOR_META_LENGTH]);
                raf.setLength(HEADER_FOR_META_LENGTH);

                log.info("Index clear reverse index chunk: worker-" + workerId + "/" + CHUNK_FILE_NAME_PREFIX + i);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        }
    }

    @Override
    public void cleanUp() {
        if (rafArray != null) {
            for (int i = 0; i < rafArray.length; i++) {
                var raf = rafArray[i];
                try {
                    raf.close();
                    System.out.println("Index close reverse index chunk: worker-" + workerId + "/" + CHUNK_FILE_NAME_PREFIX + i);
                } catch (IOException e) {
                    throw new RuntimeException(e);
                }
            }
        }
    }
}
