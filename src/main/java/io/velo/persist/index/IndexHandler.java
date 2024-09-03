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
    void checkWordLength(String word) {
        if (word.length() < 2) {
            throw new IllegalArgumentException("Word length must be greater than 1");
        }
        if (word.length() > ONE_WORD_MAX_LENGTH) {
            throw new IllegalArgumentException("Word length must be less than " + ONE_WORD_MAX_LENGTH);
        }
    }

    @TestOnly
    void putWordIfNotExist(String word) {
        checkWordLength(word);

        var lowerCaseWord = word.toLowerCase();
        var segmentIndex = reverseIndexChunk.initMetaForOneWord(lowerCaseWord);
        metaIndexWords.putWord(lowerCaseWord, segmentIndex);
    }

    @TestOnly
    void addLongId(String word, long longId) {
        checkWordLength(word);

        var lowerCaseWord = word.toLowerCase();
        reverseIndexChunk.addLongId(lowerCaseWord, longId);
    }

    public void putWordAndAddLongId(String word, long longId) {
        checkWordLength(word);

        var lowerCaseWord = word.toLowerCase();
        var segmentIndex = reverseIndexChunk.initMetaForOneWord(lowerCaseWord);
        metaIndexWords.putWord(lowerCaseWord, segmentIndex);
        reverseIndexChunk.addLongId(lowerCaseWord, longId);
    }

    public TreeSet<Long> getLongIds(String word, int offset, int limit) {
        checkWordLength(word);

        var lowerCaseWord = word.toLowerCase();
        return reverseIndexChunk.getLongIds(lowerCaseWord, offset, limit);
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
