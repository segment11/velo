package io.velo.persist.index;

import io.activej.common.function.RunnableEx;
import io.activej.config.Config;
import io.activej.eventloop.Eventloop;
import io.activej.promise.Promise;
import io.velo.NeedCleanUp;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

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
    private ReverseIndexChunk reverseIndexChunk;

    void initChunk(byte fdPerChunk, File workerIdDir, Config persistConfig) throws IOException {
        this.metaIndexWords = new MetaIndexWords(workerId, workerIdDir);
        this.reverseIndexChunk = new ReverseIndexChunk(workerId, workerIdDir, fdPerChunk, persistConfig);
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
        System.out.println("Index handler begin to clean up: " + workerId);

        if (metaIndexWords != null) {
            metaIndexWords.cleanUp();
        }
        if (reverseIndexChunk != null) {
            reverseIndexChunk.cleanUp();
        }
    }
}
