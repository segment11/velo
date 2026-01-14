package io.velo.repl;

import io.velo.ConfForSlot;
import io.velo.NeedCleanUp;
import io.velo.persist.DynConfig;
import io.velo.persist.InMemoryEstimate;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/**
 * Binlog for replication, slave pull mode, each one slot has one binlog.
 */
public class Binlog implements InMemoryEstimate, NeedCleanUp {

    private final short slot;
    private final DynConfig dynConfig;
    private File binlogDir;
    private RandomAccessFile raf;

    /**
     * Old binlog files, key is file index
     */
    private final TreeMap<Integer, RandomAccessFile> prevRafByFileIndex = new TreeMap<>();

    // for cache, ignore when pure memory mode
    private final LinkedList<BytesWithFileIndexAndOffset> latestAppendForReadCacheSegmentBytesSet = new LinkedList<>();
    private final short forReadCacheSegmentMaxCount;

    private static final String BINLOG_DIR_NAME = "binlog";

    private long diskUsage = 0L;

    /**
     * Get disk usage for stats
     *
     * @return disk usage
     */
    public long getDiskUsage() {
        return diskUsage;
    }

    /**
     * For cache when slave pull, last written segment bytes
     *
     * @param bytes     binlog one segment bytes
     * @param fileIndex file index
     * @param offset    file offset
     */
    @VisibleForTesting
    record BytesWithFileIndexAndOffset(byte[] bytes, int fileIndex,
                                       long offset) implements Comparable<BytesWithFileIndexAndOffset> {
        @Override
        public @NotNull String toString() {
            return "BytesWithFileIndexAndOffset{" +
                    "fileIndex=" + fileIndex +
                    ", offset=" + offset +
                    ", bytes.length=" + bytes.length +
                    '}';
        }

        @Override
        public int compareTo(@NotNull Binlog.BytesWithFileIndexAndOffset o) {
            if (fileIndex != o.fileIndex) {
                return Integer.compare(fileIndex, o.fileIndex);
            }
            return Long.compare(offset, o.offset);
        }
    }

    /**
     * File index and offset, for one repl pair mark
     *
     * @param fileIndex file index
     * @param offset    file offset
     */
    public record FileIndexAndOffset(int fileIndex, long offset) {
        @Override
        public @NotNull String toString() {
            return "FileIndexAndOffset{" +
                    "fileIndex=" + fileIndex +
                    ", offset=" + offset +
                    '}';
        }

        @Override
        public boolean equals(Object obj) {
            if (obj == null) {
                return false;
            }
            if (getClass() != obj.getClass()) {
                return false;
            }
            FileIndexAndOffset that = (FileIndexAndOffset) obj;
            return fileIndex == that.fileIndex && offset == that.offset;
        }

        public long asReplOffset() {
            return (long) fileIndex * ConfForSlot.global.confRepl.binlogOneFileMaxLength + offset;
        }
    }

    private static final Logger log = LoggerFactory.getLogger(Binlog.class);

    /**
     * List binlog files
     *
     * @return binlog files, sorted by file index
     */
    private ArrayList<PersistFile> listFiles() {
        ArrayList<PersistFile> list = new ArrayList<>();
        var files = binlogDir.listFiles();
        if (files == null) {
            return list;
        }

        for (var file : files) {
            if (file.getName().startsWith(FILE_NAME_PREFIX)) {
                list.add(new PersistFile(file, fileIndex(file)));
            }
        }
        list.sort(Comparator.comparingInt(PersistFile::fileIndex));
        return list;
    }

    /**
     * Constructor
     *
     * @param slot      slot
     * @param slotDir   the directory of the slot
     * @param dynConfig dyn config
     * @throws IOException when io error
     */
    public Binlog(short slot, @NotNull File slotDir, @NotNull DynConfig dynConfig) throws IOException {
        this.slot = slot;
        this.dynConfig = dynConfig;

        this.binlogDir = new File(slotDir, BINLOG_DIR_NAME);
        if (!binlogDir.exists()) {
            if (!binlogDir.mkdirs()) {
                throw new IOException("Repl create binlog dir error, slot=" + slot);
            }
        }

        PersistFile latestFile;
        var files = listFiles();
        if (!files.isEmpty()) {
            latestFile = files.getLast();
            this.currentFileIndex = latestFile.fileIndex();
            this.currentFileOffset = latestFile.length();

            for (var file : files) {
                this.diskUsage += file.length();
            }
        } else {
            // begin from 0
            File file = new File(binlogDir, fileName(currentFileIndex));
            FileUtils.touch(file);
            latestFile = new PersistFile(file, 0);
        }
        this.raf = new RandomAccessFile(latestFile.file(), "rw");

        this.forReadCacheSegmentMaxCount = ConfForSlot.global.confRepl.binlogForReadCacheSegmentMaxCount;
        this.tempAppendSegmentBytes = new byte[ConfForSlot.global.confRepl.binlogOneSegmentLength];
        this.tempAppendSegmentBuffer = ByteBuffer.wrap(tempAppendSegmentBytes);
    }

    /**
     * Estimate memory cost size
     *
     * @param sb string builder
     * @return memory cost size
     */
    @Override
    public long estimate(@NotNull StringBuilder sb) {
        long size = 0;
        size += tempAppendSegmentBytes.length;
        for (var one : latestAppendForReadCacheSegmentBytesSet) {
            size += one.bytes.length;
        }

        sb.append("Binlog buffer: ").append(size).append("\n");
        return size;
    }

    /**
     * Get current file index
     *
     * @return current file index
     */
    @TestOnly
    int getCurrentFileIndex() {
        return currentFileIndex;
    }

    /**
     * Get current file offset
     *
     * @return current file offset
     */
    @TestOnly
    long getCurrentFileOffset() {
        return currentFileOffset;
    }

    private int currentFileIndex = 0;

    private long currentFileOffset = 0;

    @Override
    public String toString() {
        return "Binlog{" +
                "slot=" + slot +
                ", binlogDir=" + binlogDir +
                ", currentFileIndex=" + currentFileIndex +
                ", currentFileOffset=" + currentFileOffset +
                ", latestAppendForReadCacheSegmentBytesSet.size=" + latestAppendForReadCacheSegmentBytesSet.size() +
                ", prevRafByFileIndex.size=" + prevRafByFileIndex.size() +
                '}';
    }

    /**
     * Get current file index and offset wrap
     *
     * @return one file index and offset object
     */
    public FileIndexAndOffset currentFileIndexAndOffset() {
        return new FileIndexAndOffset(currentFileIndex, currentFileOffset);
    }

    /**
     * Get current repl offset
     *
     * @return current repl offset, calculated by file index and offset
     */
    public long currentReplOffset() {
        var binlogOneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;
        return (long) currentFileIndex * binlogOneFileMaxLength + currentFileOffset;
    }

    /**
     * Get the earliest file index and offset, not 0 because old binlog files may be deleted
     *
     * @return the earliest file index and offset
     */
    public FileIndexAndOffset earliestFileIndexAndOffset() {
        // at least have one file, self created
        var files = listFiles();
        var file = files.getFirst();
        return new FileIndexAndOffset(file.fileIndex(), 0);
    }

    @TestOnly
    void resetCurrentFileOffset(long offset) {
        this.currentFileOffset = offset;
        this.clearByteBuffer();
    }

    /**
     * For padding one complete segment
     */
    @VisibleForTesting
    static class PaddingBinlogContent implements BinlogContent {
        private final byte[] paddingBytes;

        public PaddingBinlogContent(byte[] paddingBytes) {
            this.paddingBytes = paddingBytes;
        }

        @Override
        public Type type() {
            // need not decode
            return null;
        }

        @Override
        public int encodedLength() {
            return paddingBytes.length;
        }

        @Override
        public byte[] encodeWithType() {
            return paddingBytes;
        }

        @Override
        public void apply(short slot, ReplPair replPair) {
            // do nothing
        }
    }

    /**
     * Move to next segment
     *
     * @throws IOException when io error
     */
    public void moveToNextSegment() throws IOException {
        moveToNextSegment(false);
    }

    /**
     * Move to next segment
     *
     * @param forceEvenIfMargin true ignore when current offset is one segment completed
     * @throws IOException when io error
     */
    public void moveToNextSegment(boolean forceEvenIfMargin) throws IOException {
        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var mod = currentFileOffset % oneSegmentLength;
        if (mod != 0) {
            var paddingN = oneSegmentLength - mod;
            var paddingBytes = new byte[(int) paddingN];
            var paddingContent = new PaddingBinlogContent(paddingBytes);
            append(paddingContent);
        } else {
            if (!forceEvenIfMargin) {
                return;
            }

            currentFileOffset += oneSegmentLength;
            // padding
            raf.setLength(currentFileOffset);
            clearByteBuffer();

            var oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;
            var isLastSegment = currentFileOffset == oneFileMaxLength;
            if (isLastSegment) {
                createAndUseNextFile();
            }
        }
        log.warn("Repl binlog move to next segment, file index={}, offset={}, slot={}",
                currentFileIndex, currentFileOffset, slot);
    }

    /**
     * Reopen at target file index and margin offset
     *
     * @param resetFileIndex target file index
     * @param marginOffset   target file offset, align to one segment
     * @throws IOException when io error
     */
    public void reopenAtFileIndexAndMarginOffset(int resetFileIndex, long marginOffset) throws IOException {
        if (currentFileIndex == resetFileIndex) {
            // truncate to target offset
            raf.setLength(marginOffset);
            currentFileOffset = marginOffset;
            clearByteBuffer();

            log.warn("Repl binlog reopen at file index and margin offset, file index={}, offset={}, slot={}",
                    currentFileIndex, currentFileOffset, slot);
            return;
        }

        IOUtils.closeQuietly(raf);
        var rafRemoved = prevRafByFileIndex.remove(currentFileIndex);
        if (rafRemoved != null) {
            if (rafRemoved != raf) {
                IOUtils.closeQuietly(rafRemoved);
            }
        }

        var prevRaf = prevRaf(resetFileIndex, true);
        prevRaf.seek(marginOffset);

        currentFileIndex = resetFileIndex;
        currentFileOffset = marginOffset;
        clearByteBuffer();
        raf = prevRaf;

        log.warn("Repl binlog reopen at file index and margin offset, file index={}, offset={}, slot={}",
                currentFileIndex, currentFileOffset, slot);
    }

    // binlog file name prefix
    static final String FILE_NAME_PREFIX = "binlog-";

    private static String fileName(int fileIndex) {
        return FILE_NAME_PREFIX + fileIndex;
    }

    private static int fileIndex(File file) {
        return Integer.parseInt(file.getName().substring(FILE_NAME_PREFIX.length()));
    }

    // for batch
    private final byte[] tempAppendSegmentBytes;
    private final ByteBuffer tempAppendSegmentBuffer;

    private void clearByteBuffer() {
        var position = tempAppendSegmentBuffer.position();
        if (position > 0) {
            tempAppendSegmentBuffer.clear();

            Arrays.fill(tempAppendSegmentBytes, 0, position, (byte) 0);
        }
    }

    private void addForReadCacheSegmentBytes(int fileIndex, long offset, byte[] givenBytes) {
        // copy one
        byte[] bytes;
        if (givenBytes != null) {
            bytes = givenBytes;
        } else {
            bytes = new byte[tempAppendSegmentBytes.length];
            System.arraycopy(tempAppendSegmentBytes, 0, bytes, 0, tempAppendSegmentBytes.length);
        }

        if (latestAppendForReadCacheSegmentBytesSet.size() >= forReadCacheSegmentMaxCount) {
            latestAppendForReadCacheSegmentBytesSet.removeFirst();
        }
        latestAppendForReadCacheSegmentBytesSet.add(new BytesWithFileIndexAndOffset(bytes, fileIndex, offset));
    }

    public static int oneFileMaxSegmentCount() {
        return ConfForSlot.global.confRepl.binlogOneFileMaxLength / ConfForSlot.global.confRepl.binlogOneSegmentLength;
    }

    /**
     * Align to one segment
     *
     * @param fileOffset file offset
     * @return the margin offset
     */
    public static int marginFileOffset(long fileOffset) {
        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        return (int) (fileOffset - fileOffset % oneSegmentLength);
    }

    /**
     * Append binlog content
     *
     * @param content the binlog content
     * @throws IOException when io error
     */
    public void append(@NotNull BinlogContent content) throws IOException {
        if (!dynConfig.isBinlogOn()) {
            return;
        }

        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;

        var encoded = content.encodeWithType();
        if (encoded.length >= oneSegmentLength) {
            throw new IllegalArgumentException("Repl append binlog content error, encoded length must be less than one segment length, slot=" +
                    slot + ", encoded length=" + encoded.length);
        }

        var mod = currentFileOffset % oneSegmentLength;
        if (mod == 0 && currentFileOffset != 0) {
            addForReadCacheSegmentBytes(currentFileIndex, currentFileOffset - oneSegmentLength, null);
            clearByteBuffer();
        } else {
            var currentSegmentLeft = oneSegmentLength - mod;
            var isCrossSegment = encoded.length > currentSegmentLeft;
            if (isCrossSegment) {
                // need padding
                var padding = new byte[(int) currentSegmentLeft];

                raf.seek(currentFileOffset);
                raf.write(padding);
                currentFileOffset += padding.length;

                diskUsage += padding.length;

                tempAppendSegmentBuffer.put(padding);
                addForReadCacheSegmentBytes(currentFileIndex, currentFileOffset - oneSegmentLength, null);
                clearByteBuffer();

                if (currentFileOffset == oneFileMaxLength) {
                    createAndUseNextFile();
                }
            }
        }

        raf.seek(currentFileOffset);
        raf.write(encoded);
        currentFileOffset += encoded.length;

        diskUsage += encoded.length;

        tempAppendSegmentBuffer.put(encoded);

        if (currentFileOffset == oneFileMaxLength) {
            addForReadCacheSegmentBytes(currentFileIndex, currentFileOffset - oneSegmentLength, null);
            clearByteBuffer();

            createAndUseNextFile();
        }
    }

    /**
     * Write binlog one segment bytes received from master, as slave, but also as master to another slave, need do binlog just same as master
     *
     * @param oneSegmentBytes one segment bytes
     * @param toFileIndex     to file index
     * @param toFileOffset    to file offset
     * @throws IOException when io error
     */
    public void writeFromMasterOneSegmentBytes(byte[] oneSegmentBytes, int toFileIndex, long toFileOffset) throws IOException {
        if (!dynConfig.isBinlogOn()) {
            return;
        }

        int binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        if (oneSegmentBytes.length > binlogOneSegmentLength) {
            throw new IllegalArgumentException("Repl write binlog one segment bytes error, length must be less than " + binlogOneSegmentLength + ", slot=" + slot);
        }

        var oneFileMaxLength = ConfForSlot.global.confRepl.binlogOneFileMaxLength;

        if (currentFileIndex == toFileIndex) {
            raf.seek(toFileOffset);
            raf.write(oneSegmentBytes);

            diskUsage += oneSegmentBytes.length;

            // because when master reset, self will write binlog from wal
            // need margin so can use a new beginning segment
            currentFileOffset = toFileOffset + binlogOneSegmentLength;
            clearByteBuffer();

            var isLastSegment = currentFileOffset == oneFileMaxLength;
            if (isLastSegment) {
                createAndUseNextFile();
            }
        } else {
            var prevRaf = prevRaf(toFileIndex, true);

            prevRaf.seek(toFileOffset);
            prevRaf.write(oneSegmentBytes);

            diskUsage += oneSegmentBytes.length;

            if (currentFileIndex < toFileIndex) {
                IOUtils.closeQuietly(raf);
                var rafRemoved = prevRafByFileIndex.remove(currentFileIndex);
                if (rafRemoved != null) {
                    if (rafRemoved != raf) {
                        IOUtils.closeQuietly(rafRemoved);
                    }
                }

                // set current to latest, because when master reset, self will write binlog from wal
                // need margin so can use a new beginning segment
                currentFileIndex = toFileIndex;
                currentFileOffset = toFileOffset + binlogOneSegmentLength;
                clearByteBuffer();
                raf = prevRaf;

                var isLastSegment = currentFileOffset == oneFileMaxLength;
                if (isLastSegment) {
                    createAndUseNextFile();
                }
            }
        }

        addForReadCacheSegmentBytes(toFileIndex, toFileOffset, oneSegmentBytes);
    }

    private void createAndUseNextFile() throws IOException {
        raf.close();
        log.info("Repl close current binlog file as overflow, file={}, slot={}", fileName(currentFileIndex), slot);

        var prevRaf = prevRafByFileIndex.remove(currentFileIndex);
        if (prevRaf != null) {
            if (prevRaf != raf) {
                IOUtils.closeQuietly(prevRaf);
            }
        }

        currentFileIndex++;

        var nextFile = new File(binlogDir, fileName(currentFileIndex));
        FileUtils.touch(nextFile);
        log.info("Repl create new binlog file, file={}, slot={}", nextFile.getName(), slot);
        raf = new RandomAccessFile(nextFile, "rw");

        // reset offset beginning at 0
        currentFileOffset = 0;

        // check file keep max count
        var files = listFiles();
        if (files.size() > ConfForSlot.global.confRepl.binlogFileKeepMaxCount) {
            // already sorted
            var firstFile = files.getFirst();
            var rafRemoved = prevRafByFileIndex.remove(firstFile.fileIndex());
            if (rafRemoved != null) {
                rafRemoved.close();
                log.info("Repl close binlog old raf success, file index={}, slot={}", firstFile.fileIndex(), slot);
            }

            var fileLength = firstFile.length();
            if (!firstFile.delete()) {
                log.error("Repl delete binlog file error, file={}, slot={}", firstFile.getName(), slot);
            } else {
                log.info("Repl delete binlog file success, file={}, slot={}", firstFile.getName(), slot);
                diskUsage -= fileLength;
            }
        }
    }

    @VisibleForTesting
    RandomAccessFile prevRaf(int fileIndex) {
        return prevRaf(fileIndex, false);
    }

    private RandomAccessFile prevRaf(int fileIndex, boolean createIfNotExists) {
        if (fileIndex < 0) {
            throw new IllegalArgumentException("Repl read binlog prev raf, file index must be >= 0, slot=" + slot);
        }

        return prevRafByFileIndex.computeIfAbsent(fileIndex, k -> {
            var file = new File(binlogDir, FILE_NAME_PREFIX + fileIndex);
            if (!file.exists()) {
                if (!createIfNotExists) {
                    return null;
                } else {
                    try {
                        FileUtils.touch(file);
                    } catch (IOException e) {
                        throw new RuntimeException("Repl touch new binlog file error, file index=" + fileIndex + ", slot=" + slot, e);
                    }
                }
            }

            try {
                return new RandomAccessFile(file, "rw");
            } catch (FileNotFoundException e) {
                // never happen
                throw new RuntimeException(e);
            }
        });
    }

    private byte[] getLatestAppendForReadCacheSegmentBytes(int fileIndex, long offset) {
        var one = Collections.binarySearch(latestAppendForReadCacheSegmentBytesSet,
                new BytesWithFileIndexAndOffset(null, fileIndex, offset));
        return one >= 0 ? latestAppendForReadCacheSegmentBytesSet.get(one).bytes : null;
    }

    /**
     * The total time spent reading (in microseconds).
     */
    private long readTimeTotalUs;

    /**
     * The total count of read operations.
     */
    private long readCountTotal;

    /**
     * Get the average time spent reading (in microseconds).
     *
     * @return the average time spent reading (in microseconds)
     */
    public double getReadTimeAvgUs() {
        if (readCountTotal == 0) {
            return 0;
        }
        return (double) readTimeTotalUs / readCountTotal;
    }

    /**
     * Read one segment bytes from previous binlog file
     *
     * @param fileIndex the file index
     * @param offset    the file offset
     * @return one segment bytes
     * @throws IOException when read error
     */
    public byte[] readPrevRafOneSegment(int fileIndex, long offset) throws IOException {
        if (fileIndex < 0) {
            throw new IllegalArgumentException("Repl read binlog segment bytes, file index must be >= 0, slot=" + slot);
        }

        var oneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var modGiven = offset % oneSegmentLength;
        if (modGiven != 0) {
            throw new IllegalArgumentException("Repl read binlog segment bytes, offset must be multiple of one segment length, offset=" + offset + ", slot=" + slot);
        }

        // get from cache first, only consider current file or previous file
        if (currentFileIndex == fileIndex || currentFileIndex - 1 == fileIndex) {
            // check cache
            var segmentBytes = getLatestAppendForReadCacheSegmentBytes(fileIndex, offset);
            if (segmentBytes != null) {
                return segmentBytes;
            }
        }

        // need not close
        var prevRaf = prevRaf(fileIndex);
        if (prevRaf == null) {
            // keep max count = 10 or 100, if write too fast, may be lost some files
            // so slave will get error repl reply, then need re-fetch all exists data from master and re-catch up
            throw new IOException("Repl read binlog segment bytes, file not exist, file index=" + fileIndex + ", slot=" + slot);
        }

        // when?, not any binlog will cause this, other cases need to check, todo
        if (prevRaf.length() <= offset) {
            return null;
        }

        var bytes = new byte[oneSegmentLength];

        var beginT = System.nanoTime();
        prevRaf.seek(offset);
        var n = prevRaf.read(bytes);
        var costT = (System.nanoTime() - beginT) / 1000;

        // stats
        readTimeTotalUs += costT;
        readCountTotal++;

        if (n < 0) {
            throw new IOException("Repl read binlog segment bytes error, file index=" + fileIndex + ", offset=" + offset + ", slot=" + slot);
        }
        if (n < oneSegmentLength) {
            var readBytes = new byte[n];
            System.arraycopy(bytes, 0, readBytes, 0, n);
            return readBytes;
        }
        return bytes;
    }

    /**
     * Decode from one segment bytes and apply to slave repl pair
     *
     * @param slot            the replication slot to which this content is applied
     * @param oneSegmentBytes the one segment bytes
     * @param skipBytesN      the skip bytes number
     * @param replPair        the repl pair associated with this replication session
     * @return the number of decoded content
     */
    public static int decodeAndApply(short slot,
                                     byte[] oneSegmentBytes,
                                     int skipBytesN,
                                     @NotNull ReplPair replPair) {
        var byteBuffer = ByteBuffer.wrap(oneSegmentBytes);
        byteBuffer.position(skipBytesN);

        var n = 0;
        while (true) {
            if (byteBuffer.remaining() == 0) {
                break;
            }

            var code = byteBuffer.get();
            if (code == 0) {
                break;
            }

            var type = BinlogContent.Type.fromCode(code);
            var content = type.decodeFrom(byteBuffer);
            content.apply(slot, replPair);
            n++;
        }
        return n;
    }

    /**
     * Analyse one binlog file
     *
     * @param binlogFile the binlog file
     * @return the number of decoded content
     * @throws IOException when read error
     */
    public static long analyseBinlogFile(@NotNull File binlogFile) throws IOException {
        var binlogOneSegmentLength = ConfForSlot.global.confRepl.binlogOneSegmentLength;
        var oneSegmentBytes = new byte[binlogOneSegmentLength];
        var byteBuffer = ByteBuffer.wrap(oneSegmentBytes);

        Map<BinlogContent.Type, Long> countByType = new HashMap<>();
        Map<BinlogContent.Type, Long> encodedLengthByType = new HashMap<>();

        var is = new BufferedInputStream(new FileInputStream(binlogFile));
        while (true) {
            var n = is.read(oneSegmentBytes);
            if (n < 0) {
                break;
            }
            byteBuffer.position(0);

            while (true) {
                if (byteBuffer.remaining() == 0) {
                    break;
                }

                var code = byteBuffer.get();
                if (code == 0) {
                    break;
                }

                var type = BinlogContent.Type.fromCode(code);
                var content = type.decodeFrom(byteBuffer);

                // add count
                countByType.put(type, countByType.getOrDefault(type, 0L) + 1);
                encodedLengthByType.put(type, encodedLengthByType.getOrDefault(type, 0L) + content.encodedLength());
            }

            if (n < binlogOneSegmentLength) {
                break;
            }
        }

        long totalCount = 0L;
        for (var entry : countByType.entrySet()) {
            var type = entry.getKey();
            System.out.println(type + ": count - " + entry.getValue() + ", encoded length - " + encodedLengthByType.get(type));
            totalCount += entry.getValue();
        }
        System.out.println("Total count - " + totalCount);
        return totalCount;
    }

    @SlaveNeedReplay
    @SlaveReplay
    public void truncateAll() {
        try {
            raf.setLength(0);
        } catch (IOException e) {
            log.error("Repl clear binlog raf error, file index={}, slot={}", currentFileIndex, slot, e);
        }

        var it = prevRafByFileIndex.entrySet().iterator();
        while (it.hasNext()) {
            var entry = it.next();
            var prevRaf = entry.getValue();
            IOUtils.closeQuietly(prevRaf);
            it.remove();
        }

        var files = listFiles();
        for (var file : files) {
            if (file.fileIndex() == currentFileIndex) {
                continue;
            }

            var fileLength = file.length();
            if (!file.delete()) {
                log.error("Repl delete binlog file error, file={}, slot={}", file.getName(), slot);
            } else {
                log.info("Repl delete binlog file success, file={}, slot={}", file.getName(), slot);
                diskUsage -= fileLength;
            }
        }
    }

    @Override
    public void cleanUp() {
        try {
            raf.close();
            System.out.println("Repl close binlog current raf success, slot=" + slot);
        } catch (IOException e) {
            System.err.println("Repl close binlog current raf error, slot=" + slot);
        }

        for (var entry : prevRafByFileIndex.entrySet()) {
            var prevFileIndex = entry.getKey();
            var prevRaf = entry.getValue();
            try {
                prevRaf.close();
                System.out.println("Repl close binlog old raf success, slot=" + slot + ", file index=" + prevFileIndex);
            } catch (IOException e) {
                System.err.println("Repl close binlog old raf error, slot=" + slot + ", file index=" + prevFileIndex);
            }
        }
    }
}
