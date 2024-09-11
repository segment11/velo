package io.velo.persist.index;

import io.activej.common.function.RunnableEx;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.velo.NeedCleanUp;
import io.velo.repl.MasterReset;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.util.TreeSet;

import static io.velo.persist.index.MetaIndexWords.ONE_WORD_MAX_LENGTH;

public class IndexHandler implements NeedCleanUp {
    private final byte workerId;
    private final Eventloop eventloop;

    IndexHandler(byte workerId, Eventloop eventloop) {
        this.workerId = workerId;
        this.eventloop = eventloop;
    }

    private MetaIndexWords metaIndexWords;
    @VisibleForTesting
    ReverseIndexChunk reverseIndexChunk;

    private static final Logger log = LoggerFactory.getLogger(IndexHandler.class);

    void initChunk(byte fdPerChunk, File workerIdDir, int expiredIfSecondsFromNow) throws IOException {
        this.metaIndexWords = new MetaIndexWords(workerId, workerIdDir);
        this.reverseIndexChunk = new ReverseIndexChunk(workerId, workerIdDir, fdPerChunk, expiredIfSecondsFromNow);
    }

    @MasterReset
    public void resetAsMaster() throws IOException {
        log.warn("Index reset as master begin, reload meta from exists file, worker id={}", workerId);
        metaIndexWords.reload();
        reverseIndexChunk.loadMeta();
        log.warn("Index reset as master done, worker id={}", workerId);
    }

    // for repl
    public byte[] metaIndexWordsReadOneBatch(int beginOffset, int length) {
        return metaIndexWords.readOneBatch(beginOffset, length);
    }

    public void metaIndexWordsWriteOneBatch(int beginOffset, byte[] bytes) {
        metaIndexWords.writeOneBatch(beginOffset, bytes);
    }

    public byte[] chunkReadOneSegment(int segmentIndex) {
        return reverseIndexChunk.readOneSegment(segmentIndex);
    }

    public void chunkWriteOneSegment(int segmentIndex, byte[] bytes) {
        reverseIndexChunk.writeOneSegment(segmentIndex, bytes);
    }

    public int getChunkMaxSegmentNumber() {
        return reverseIndexChunk.maxSegmentNumber;
    }

    @VisibleForTesting
    void checkWordLength(String lowerCaseWord) {
        if (lowerCaseWord.length() < 2) {
            throw new IllegalArgumentException("Word length must be greater than 1");
        }
        if (lowerCaseWord.length() > ONE_WORD_MAX_LENGTH) {
            throw new IllegalArgumentException("Word length must be less than " + ONE_WORD_MAX_LENGTH);
        }
    }

    @TestOnly
    void putWordIfNotExist(String lowerCaseWord) {
        checkWordLength(lowerCaseWord);

        var segmentIndex = reverseIndexChunk.initMetaForOneWord(lowerCaseWord);
        metaIndexWords.putWord(lowerCaseWord, segmentIndex, 0);
    }

    @TestOnly
    void addLongId(String lowerCaseWord, long longId) {
        checkWordLength(lowerCaseWord);

        var segmentIndex = reverseIndexChunk.addLongId(lowerCaseWord, longId);
        metaIndexWords.putWord(lowerCaseWord, segmentIndex, longId > 0 ? 1 : -1);
    }

    public void putWordAndAddLongId(String lowerCaseWord, long longId) {
        checkWordLength(lowerCaseWord);

        var segmentIndex = reverseIndexChunk.initMetaForOneWord(lowerCaseWord);
        // long id < 0 means delete
        metaIndexWords.putWord(lowerCaseWord, segmentIndex, longId > 0 ? 1 : -1);
        reverseIndexChunk.addLongId(lowerCaseWord, longId);
    }

    public TreeSet<Long> getLongIds(String lowerCaseWord, int offset, int limit) {
        checkWordLength(lowerCaseWord);

        return reverseIndexChunk.getLongIds(lowerCaseWord, offset, limit);
    }

    public int getTotalCount(String lowerCaseWord) {
        checkWordLength(lowerCaseWord);

        return metaIndexWords.getTotalCount(lowerCaseWord);
    }

    long threadIdProtectedForSafe = -1;

    public Promise<Void> asyncRun(RunnableEx runnableEx) {
        var threadId = Thread.currentThread().threadId();
        if (threadId == threadIdProtectedForSafe) {
            try {
                runnableEx.run();
                return Promise.complete();
            } catch (Exception e) {
                return Promise.ofException(e);
            }
        }

        return Promise.ofFuture(eventloop.submit(runnableEx));
    }

    @Override
    public void cleanUp() {
        threadIdProtectedForSafe = Thread.currentThread().threadId();
        System.out.println("Index handler begin to clean up=" + workerId);

        if (metaIndexWords != null) {
            metaIndexWords.cleanUp();
        }
        if (reverseIndexChunk != null) {
            reverseIndexChunk.cleanUp();
        }
    }
}
