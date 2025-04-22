package io.velo.persist.index;

import io.velo.ConfForGlobal;
import io.velo.NeedCleanUp;
import io.velo.extend.BetaExtend;
import io.velo.repl.MasterReset;
import io.velo.repl.SlaveNeedReplay;
import io.velo.repl.SlaveReplay;
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
import java.util.TreeMap;
import java.util.TreeSet;

@BetaExtend
public class MetaIndexWords implements NeedCleanUp {
    private static final String META_INDEX_WORDS_FILE = "meta_index_words.dat";

    private final byte workerId;
    private final File workerIdDir;

    final int allCapacity;
    private final byte[] inMemoryCachedBytes;
    private final ByteBuffer inMemoryCachedByteBuffer;
    private RandomAccessFile raf;

    // for repl
    byte[] readOneBatch(int beginOffset, int length) {
        var realReadLength = Math.min(length, allCapacity - beginOffset);
        var bytes = new byte[realReadLength];
        inMemoryCachedByteBuffer.position(beginOffset);
        inMemoryCachedByteBuffer.get(bytes);
        return bytes;
    }

    void writeOneBatch(int beginOffset, byte[] bytes) {
        if (beginOffset + bytes.length > allCapacity) {
            throw new IllegalArgumentException("Write bytes out of capacity, begin offset=" + beginOffset + ", bytes length=" + bytes.length);
        }

        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.position(beginOffset);
            inMemoryCachedByteBuffer.put(bytes);
            return;
        }

        try {
            raf.seek(beginOffset);
            raf.write(bytes);

            inMemoryCachedByteBuffer.position(beginOffset);
            inMemoryCachedByteBuffer.put(bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private static final Logger log = LoggerFactory.getLogger(MetaIndexWords.class);

    private static final int HEADER_FOR_META_LENGTH = 4096;

    // support 65536 words
    private static final int ALL_WORDS_COUNT = 65536;
    static final int ONE_WORD_MAX_LENGTH = 32;

    // first alphabet 26, second alphabet 26, length <= 8 / 16 / 32
    // not alphabet also divided into 26 * 26 groups
    private static final int ALL_WORDS_GROUP_NUMBER = 26 * 26;
    private static final int ONE_WORD_GROUP_ESTIMATE_WORD_COUNT = 64;
    private static final int ONE_WORD_GROUP_WORD_LENGTH8_OFFSET;
    private static final int ONE_WORD_GROUP_WORD_LENGTH16_OFFSET;
    private static final int ONE_WORD_GROUP_WORD_LENGTH32_OFFSET;
    private static final int ONE_GROUP_NOT_ALPHABET_WORD_LENGTH8_OFFSET;
    private static final int ONE_GROUP_NOT_ALPHABET_WORD_LENGTH16_OFFSET;
    private static final int ONE_GROUP_NOT_ALPHABET_WORD_LENGTH32_OFFSET;

    private static final int ONE_GROUP_OFFSET;

    /**
     * real word length short + int id + segment index int + total count int
     */
    private static final int ONE_WORD_META_LENGTH = 2 + 4 + 4 + 4;

    static {
        ONE_WORD_GROUP_WORD_LENGTH8_OFFSET = 0;
        ONE_WORD_GROUP_WORD_LENGTH16_OFFSET = ONE_WORD_GROUP_WORD_LENGTH8_OFFSET + (ONE_WORD_META_LENGTH + 8) * ONE_WORD_GROUP_ESTIMATE_WORD_COUNT;
        log.info("Index, one word group length <= 8 take {} bytes", ONE_WORD_GROUP_WORD_LENGTH16_OFFSET);
        ONE_WORD_GROUP_WORD_LENGTH32_OFFSET = ONE_WORD_GROUP_WORD_LENGTH16_OFFSET + (ONE_WORD_META_LENGTH + 16) * ONE_WORD_GROUP_ESTIMATE_WORD_COUNT;
        log.info("Index, one word group length <= 16 include <= 8 take {} bytes", ONE_WORD_GROUP_WORD_LENGTH32_OFFSET);

        ONE_GROUP_NOT_ALPHABET_WORD_LENGTH8_OFFSET = ONE_WORD_GROUP_WORD_LENGTH32_OFFSET + (ONE_WORD_META_LENGTH + 32) * ONE_WORD_GROUP_ESTIMATE_WORD_COUNT;
        log.info("Index, one word group length <= 32 include <= 16 take {} bytes", ONE_GROUP_NOT_ALPHABET_WORD_LENGTH8_OFFSET);
        ONE_GROUP_NOT_ALPHABET_WORD_LENGTH16_OFFSET = ONE_GROUP_NOT_ALPHABET_WORD_LENGTH8_OFFSET + (ONE_WORD_META_LENGTH + 8) * ONE_WORD_GROUP_ESTIMATE_WORD_COUNT;
        log.info("Index, one word group include not alphabet length <= 8 take {} bytes", ONE_GROUP_NOT_ALPHABET_WORD_LENGTH16_OFFSET);
        ONE_GROUP_NOT_ALPHABET_WORD_LENGTH32_OFFSET = ONE_GROUP_NOT_ALPHABET_WORD_LENGTH16_OFFSET + (ONE_WORD_META_LENGTH + 16) * ONE_WORD_GROUP_ESTIMATE_WORD_COUNT;
        log.info("Index, one word group include not alphabet length <= 16 take {} bytes", ONE_GROUP_NOT_ALPHABET_WORD_LENGTH32_OFFSET);

        ONE_GROUP_OFFSET = ONE_GROUP_NOT_ALPHABET_WORD_LENGTH32_OFFSET + (ONE_WORD_META_LENGTH + 32) * ONE_WORD_GROUP_ESTIMATE_WORD_COUNT;
        log.info("Index, one word group include not alphabet length <= 32 take {} bytes", ONE_GROUP_OFFSET);
    }

    private int intIdForOneWord = 0;

    // begin from 1
    private int generateIntIdForOneWord() {
        intIdForOneWord++;
        return intIdForOneWord;
    }

    @VisibleForTesting
    int wordGroupOffsetAfterHeaderForMeta(byte firstByte, byte secondByte, byte wordLength) {
        // lower case alphabet
        if (firstByte >= 'A' && firstByte <= 'Z') {
            firstByte += 32;
        }
        if (secondByte >= 'A' && secondByte <= 'Z') {
            secondByte += 32;
        }
        var isFirstByteAlphabet = firstByte >= 'a' && firstByte <= 'z';
        var isSecondByteAlphabet = secondByte >= 'a' && secondByte <= 'z';

        if (isFirstByteAlphabet && isSecondByteAlphabet) {
            // alphabet word use 26 * 26 groups
            int wordGroupIndex = (firstByte - 'a') * 26 + (secondByte - 'a');
            int wordGroupOffset = wordGroupIndex * ONE_GROUP_OFFSET;
            if (wordLength <= 8) {
                return ONE_WORD_GROUP_WORD_LENGTH8_OFFSET + wordGroupOffset;
            } else if (wordLength <= 16) {
                return ONE_WORD_GROUP_WORD_LENGTH16_OFFSET + wordGroupOffset;
            } else {
                return ONE_WORD_GROUP_WORD_LENGTH32_OFFSET + wordGroupOffset;
            }
        } else {
            // not alphabet word divide into 26 * 26 groups
            int wordGroupIndex = (firstByte * 128 + secondByte) % ALL_WORDS_GROUP_NUMBER;
            int wordGroupOffset = wordGroupIndex * ONE_GROUP_OFFSET;
            if (wordLength <= 8) {
                return ONE_GROUP_NOT_ALPHABET_WORD_LENGTH8_OFFSET + wordGroupOffset;
            } else if (wordLength <= 16) {
                return ONE_GROUP_NOT_ALPHABET_WORD_LENGTH16_OFFSET + wordGroupOffset;
            } else {
                return ONE_GROUP_NOT_ALPHABET_WORD_LENGTH32_OFFSET + wordGroupOffset;
            }
        }
    }

    public MetaIndexWords(byte workerId, File workerIdDir) throws IOException {
        this.workerId = workerId;
        this.workerIdDir = workerIdDir;
        this.inMemoryCachedBytes = new byte[HEADER_FOR_META_LENGTH + ONE_GROUP_OFFSET * ALL_WORDS_GROUP_NUMBER];
        log.warn("Index meta index words init size={}KB, worker id={}", inMemoryCachedBytes.length / 1024, workerId);
        this.allCapacity = inMemoryCachedBytes.length;

        if (ConfForGlobal.pureMemory) {
            this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
            return;
        }

        boolean needRead = false;
        var file = new File(workerIdDir, META_INDEX_WORDS_FILE);
        if (!file.exists()) {
            FileUtils.touch(file);
            FileUtils.writeByteArrayToFile(file, this.inMemoryCachedBytes, true);
        } else {
            needRead = true;
        }
        this.raf = new RandomAccessFile(file, "rw");

        this.inMemoryCachedByteBuffer = ByteBuffer.wrap(inMemoryCachedBytes);
        if (needRead) {
            reload();
        }
    }

    @MasterReset
    void reload() throws IOException {
        if (ConfForGlobal.pureMemory) {
            // todo
            return;
        }

        raf.seek(0);
        raf.read(inMemoryCachedBytes);
        log.warn("Index read meta index words file success, worker id={}", workerId);

        iterate((lowerCaseWord, wordMeta) -> {
            var arr = new int[2];
            arr[0] = wordMeta.segmentIndex;
            arr[1] = wordMeta.totalCount;
            afterPutWordToSegmentIndex.put(lowerCaseWord, arr);

            if (intIdForOneWord < wordMeta.intId) {
                intIdForOneWord = wordMeta.intId;
            }
        });
        log.warn("Index meta index words loaded, word count={}, worker id={}", afterPutWordToSegmentIndex.size(), workerId);
    }

    @Override
    public String toString() {
        return "MetaIndexWords{" +
                "intIdForOneWord=" + intIdForOneWord +
                ", afterPutWordSet.size=" + afterPutWordToSegmentIndex.size() +
                ", allMemoryCachedBytes.length=" + inMemoryCachedBytes.length +
                '}';
    }

    @VisibleForTesting
    int wordLength8or16or32(byte wordLength) {
        if (wordLength <= 8) {
            return 8;
        } else if (wordLength <= 16) {
            return 16;
        } else {
            return 32;
        }
    }

    record WordMeta(int intId, int segmentIndex, int totalCount, int offset) {
    }

    record WordOffsetMeta(int wordLength8or16or32, int offset) {
    }

    private WordOffsetMeta getOneWordOffsetMeta(String lowerCaseWord) {
        var bytes = lowerCaseWord.getBytes();
        var firstByte = bytes[0];
        var secondByte = bytes[1];

        var wordLength = (byte) lowerCaseWord.length();
        var wordGroupOffset = HEADER_FOR_META_LENGTH + wordGroupOffsetAfterHeaderForMeta(firstByte, secondByte, wordLength);
        var wordLength8or16or32 = wordLength8or16or32(wordLength);

        // find the first empty word
        int j = -1;
        int oneWordOffset = wordGroupOffset;
        for (int i = 0; i < ONE_WORD_GROUP_ESTIMATE_WORD_COUNT; i++) {
            var realWordLength = inMemoryCachedByteBuffer.getShort(oneWordOffset);
            if (realWordLength == 0) {
                break;
            }

            if (realWordLength != wordLength) {
                oneWordOffset += ONE_WORD_META_LENGTH + wordLength8or16or32;
                continue;
            }

            var realWordBytes = new byte[wordLength];
            inMemoryCachedByteBuffer.get(oneWordOffset + ONE_WORD_META_LENGTH, realWordBytes);
            if (!Arrays.equals(realWordBytes, bytes)) {
                oneWordOffset += ONE_WORD_META_LENGTH + wordLength8or16or32;
                continue;
            }

            // word bytes match
            j = i;
            break;
        }

        if (j == -1) {
            return null;
        }

        return new WordOffsetMeta(wordLength8or16or32, wordGroupOffset + j * (ONE_WORD_META_LENGTH + wordLength8or16or32));
    }

    private WordOffsetMeta findOneWordOffsetMetaToPut(String lowerCaseWord) {
        var bytes = lowerCaseWord.getBytes();
        var firstByte = bytes[0];
        var secondByte = bytes[1];

        var wordLength = (byte) lowerCaseWord.length();
        var wordGroupOffset = HEADER_FOR_META_LENGTH + wordGroupOffsetAfterHeaderForMeta(firstByte, secondByte, wordLength);
        var wordLength8or16or32 = wordLength8or16or32(wordLength);

        // find the first empty word
        int j = -1;
        int oneWordOffset = wordGroupOffset;
        for (int i = 0; i < ONE_WORD_GROUP_ESTIMATE_WORD_COUNT; i++) {
            var existWordLength = inMemoryCachedByteBuffer.getShort(oneWordOffset);
            if (existWordLength == 0) {
                j = i;
                break;
            }
            oneWordOffset += ONE_WORD_META_LENGTH + wordLength8or16or32;
        }

        if (j == -1) {
            return null;
        }

        return new WordOffsetMeta(wordLength8or16or32, wordGroupOffset + j * (ONE_WORD_META_LENGTH + wordLength8or16or32));
    }

    @TestOnly
    WordMeta getOneWordMeta(String lowerCaseWord) {
        var oneWordOffsetMeta = getOneWordOffsetMeta(lowerCaseWord);
        if (oneWordOffsetMeta == null) {
            return null;
        }

        var intId = inMemoryCachedByteBuffer.getInt(oneWordOffsetMeta.offset + 2);
        var segmentIndex = inMemoryCachedByteBuffer.getInt(oneWordOffsetMeta.offset + 6);
        var totalCount = inMemoryCachedByteBuffer.getInt(oneWordOffsetMeta.offset + 10);
        return new WordMeta(intId, segmentIndex, totalCount, oneWordOffsetMeta.offset);
    }

    // key is lower case word
    // value is [segment index int, total count int]
    private final TreeMap<String, int[]> afterPutWordToSegmentIndex = new TreeMap<>();

    public int afterPutWordCount() {
        return afterPutWordToSegmentIndex.size();
    }

    public int getTotalCount(String lowerCaseWord) {
        var existArr = afterPutWordToSegmentIndex.get(lowerCaseWord);
        if (existArr == null) {
            return 0;
        }
        return existArr[1];
    }

    private interface IterateCallback {
        void callback(String lowerCaseWord, WordMeta wordMeta);
    }

    private void iterate(IterateCallback callback) {
        for (int i = 0; i < ALL_WORDS_GROUP_NUMBER; i++) {
            var wordGroupOffset = HEADER_FOR_META_LENGTH + i * ONE_GROUP_OFFSET;
            iterateOneWordGroup(wordGroupOffset, callback);
        }
    }

    private void iterateOneWordGroup(int wordGroupOffset, IterateCallback callback) {
        iterateWordLength8or16or32(wordGroupOffset, true, 8, callback);
        iterateWordLength8or16or32(wordGroupOffset, true, 16, callback);
        iterateWordLength8or16or32(wordGroupOffset, true, 32, callback);
        // not alphabet
        iterateWordLength8or16or32(wordGroupOffset, false, 8, callback);
        iterateWordLength8or16or32(wordGroupOffset, false, 16, callback);
        iterateWordLength8or16or32(wordGroupOffset, false, 32, callback);
    }

    private void iterateWordLength8or16or32(int wordGroupOffset, boolean isAlphabet, int wordLength8or16or32, IterateCallback callback) {
        int byLengthOffset;
        if (isAlphabet) {
            byLengthOffset = wordLength8or16or32 == 8 ? ONE_WORD_GROUP_WORD_LENGTH8_OFFSET :
                    (wordLength8or16or32 == 16 ? ONE_WORD_GROUP_WORD_LENGTH16_OFFSET : ONE_WORD_GROUP_WORD_LENGTH32_OFFSET);
        } else {
            byLengthOffset = wordLength8or16or32 == 8 ? ONE_GROUP_NOT_ALPHABET_WORD_LENGTH8_OFFSET :
                    (wordLength8or16or32 == 16 ? ONE_GROUP_NOT_ALPHABET_WORD_LENGTH16_OFFSET : ONE_GROUP_NOT_ALPHABET_WORD_LENGTH32_OFFSET);
        }

        for (int i = 0; i < ONE_WORD_GROUP_ESTIMATE_WORD_COUNT; i++) {
            var oneWordOffset = wordGroupOffset + byLengthOffset + i * (ONE_WORD_META_LENGTH + wordLength8or16or32);
            var oneWordLength = inMemoryCachedByteBuffer.getShort(oneWordOffset);
            if (oneWordLength == 0) {
                break;
            }

            var oneWordMeta = new WordMeta(
                    inMemoryCachedByteBuffer.getInt(oneWordOffset + 2),
                    inMemoryCachedByteBuffer.getInt(oneWordOffset + 6),
                    inMemoryCachedByteBuffer.getInt(oneWordOffset + 10),
                    oneWordOffset
            );

            var oneWordBytes = new byte[oneWordLength];
            inMemoryCachedByteBuffer.get(oneWordOffset + ONE_WORD_META_LENGTH, oneWordBytes);

            callback.callback(new String(oneWordBytes), oneWordMeta);
        }
    }

    private void updateMetaTotalCount(String lowerCaseWord, int addCount) {
        if (addCount == 0) {
            return;
        }

        var oneWordMeta = getOneWordMeta(lowerCaseWord);
//        if (oneWordMeta == null) {
//            throw new IllegalStateException("Word=" + lowerCaseWord + " not exist");
//        }
        var totalCount = oneWordMeta.totalCount + addCount;
        updateOneWordMetaTotalCount(oneWordMeta, totalCount);
    }

    void putWord(String lowerCaseWord, int segmentIndex, int addCount) {
        var existArr = afterPutWordToSegmentIndex.get(lowerCaseWord);
        if (existArr != null) {
            if (existArr[0] == segmentIndex) {
                updateMetaTotalCount(lowerCaseWord, addCount);
                existArr[1] += addCount;
                return;
            } else {
                throw new IllegalStateException("Word=" + lowerCaseWord + " already exist, but segment index not match, exist=" +
                        existArr[0] + ", new=" + segmentIndex);
            }
        }

//        var oneWordOffsetMeta = getOneWordOffsetMeta(lowerCaseWord);
//        if (oneWordOffsetMeta != null) {
//            var existSegmentIndex2 = inMemoryCachedByteBuffer.getInt(oneWordOffsetMeta.offset + 6);
//            if (existSegmentIndex2 == segmentIndex) {
//                return;
//            } else {
//                throw new IllegalStateException("Word=" + lowerCaseWord + " already exist, but segment index not match, exist=" +
//                        existSegmentIndex2 + ", new=" + segmentIndex);
//            }
//        }

        var oneWordOffsetMetaToPut = findOneWordOffsetMetaToPut(lowerCaseWord);
        if (oneWordOffsetMetaToPut == null) {
            throw new IllegalStateException("No empty left for word=" + lowerCaseWord);
        }

        // add count should >= 0
        putOneWord(lowerCaseWord, oneWordOffsetMetaToPut.offset, oneWordOffsetMetaToPut.wordLength8or16or32, segmentIndex, addCount);
        var arr = new int[2];
        arr[0] = segmentIndex;
        arr[1] = addCount;
        afterPutWordToSegmentIndex.put(lowerCaseWord, arr);
    }

    @TestOnly
    void putWords(TreeSet<String> wordSet) {
        for (var lowerCaseWord : wordSet) {
            var oneWordOffsetMeta = getOneWordOffsetMeta(lowerCaseWord);
            if (oneWordOffsetMeta != null) {
                continue;
            }

            var oneWordOffsetMetaToPut = findOneWordOffsetMetaToPut(lowerCaseWord);
            if (oneWordOffsetMetaToPut == null) {
                throw new IllegalStateException("No empty left for word=" + lowerCaseWord);
            }
            putOneWord(lowerCaseWord, oneWordOffsetMetaToPut.offset, oneWordOffsetMetaToPut.wordLength8or16or32, 0, 0);
            afterPutWordToSegmentIndex.put(lowerCaseWord, new int[]{0, 0});
        }
    }

    private void putOneWord(String lowerCaseWord, int offset, int wordLength8or16or32, int segmentIndex, int initCount) {
        var bytes = new byte[ONE_WORD_META_LENGTH + wordLength8or16or32];
        var tmpBuffer = ByteBuffer.wrap(bytes);
        tmpBuffer.putShort((short) lowerCaseWord.length());
        tmpBuffer.putInt(generateIntIdForOneWord());
        tmpBuffer.putInt(segmentIndex);
        tmpBuffer.putInt(initCount);
        tmpBuffer.put(lowerCaseWord.getBytes());

        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.put(offset, bytes);
            return;
        }

        try {
            raf.seek(offset);
            raf.write(bytes);
            inMemoryCachedByteBuffer.put(offset, bytes);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private void updateOneWordMetaTotalCount(WordMeta oneWordMeta, int totalCount) {
        if (ConfForGlobal.pureMemory) {
            inMemoryCachedByteBuffer.putInt(oneWordMeta.offset + 10, totalCount);
            return;
        }

        try {
            raf.seek(oneWordMeta.offset + 10);
            raf.writeInt(totalCount);
            inMemoryCachedByteBuffer.putInt(oneWordMeta.offset + 10, totalCount);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @SlaveNeedReplay
    @SlaveReplay
    void clear() {
        if (ConfForGlobal.pureMemory) {
            Arrays.fill(inMemoryCachedBytes, (byte) 0);
            inMemoryCachedByteBuffer.clear();
            System.out.println("Index meta index words clear done, set 0 from the beginning. worker id=" + workerId);
            return;
        }

        try {
            raf.setLength(0);
            System.out.println("Index meta index words file truncated, worker id=" + workerId);

            Arrays.fill(inMemoryCachedBytes, (byte) 0);
            inMemoryCachedByteBuffer.clear();
            System.out.println("Index meta index words clear done, set 0 from the beginning. worker id=" + workerId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    @Override
    public void cleanUp() {
        if (ConfForGlobal.pureMemory) {
            return;
        }

        // sync all
        try {
//            raf.getFD().sync();
            raf.close();
            System.out.println("Index meta index words file closed, worker id=" + workerId);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
