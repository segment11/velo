package io.velo.command;

import io.activej.net.socket.tcp.ITcpSocket;
import io.activej.promise.Promise;
import io.activej.promise.Promises;
import io.activej.promise.SettablePromise;
import io.velo.BaseCommand;
import io.velo.ConfForGlobal;
import io.velo.KeyHash;
import io.velo.MultiWorkerServer;
import io.velo.reply.*;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.TreeSet;

public class VGroup extends BaseCommand {
    public VGroup(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public ArrayList<SlotWithKeyHash> parseSlots(String cmd, byte[][] data, int slotNumber) {
        ArrayList<SlotWithKeyHash> slotWithKeyHashList = new ArrayList<>();

        if (cmd.equals(REVERSE_INDEX_DOC_ADD_OR_QUERY_CMD)) {
            if (data.length < 3) {
                return slotWithKeyHashList;
            }

            var subCmd = new String(data[1]);
            if ("add".equals(subCmd)) {
                var longId = snowFlake.nextId();
                var key = REVERSE_INDEX_DOC_KEY_PREFIX + longId;
                var tmpSlot = slot(key.getBytes(), slotNumber);

                // use long id as key hash for tmp save
                slotWithKeyHashList.add(new SlotWithKeyHash(tmpSlot.slot(), tmpSlot.bucketIndex(), longId));
                return slotWithKeyHashList;
            }
        }

        return slotWithKeyHashList;
    }

    private static final String REVERSE_INDEX_DOC_ADD_OR_QUERY_CMD = "vv";
    public static final String REVERSE_INDEX_DOC_KEY_PREFIX = "_Vv_";
    // todo
    private static final int limit = 10;

    public Reply handle() {
        if (cmd.equals(REVERSE_INDEX_DOC_ADD_OR_QUERY_CMD)) {
            return vv();
        }

        return NilReply.INSTANCE;
    }

    @VisibleForTesting
    Reply vv() {
        if (data.length < 3) {
            return ErrorReply.FORMAT;
        }

        var subCmd = new String(data[1]);
        if ("add".equals(subCmd)) {
            // future: support json + json path, todo
            // eg: vv add bad,cake,daddy,fine,get
            return vv_add();
        }

        if ("query".equals(subCmd)) {
            // vv query bad
            // vv query bad&daddy
            // vv query cake|daddy
            return vv_query();
        }

        return ErrorReply.SYNTAX;
    }

    private Reply vv_query() {
        var value = new String(data[2]);

        boolean isOr = value.contains("|");
        var wordsArray = value.split(isOr ? "\\|" : "&");
        if (wordsArray.length > 2) {
            return new ErrorReply("Only support 2 words query");
        }

        var firstOneSlot = localPersist.currentThreadFirstOneSlot();

        Promise<Void>[] promises = new Promise[wordsArray.length];
        TreeSet<Long>[] returnSetArray = new TreeSet[wordsArray.length];
        for (int i = 0; i < returnSetArray.length; i++) {
            var word = wordsArray[i];
            int finalI = i;
            promises[i] = firstOneSlot.submitIndexJobRun(word, (indexHandler) -> {
                var r = indexHandler.getLongIds(word, 0, limit);
                returnSetArray[finalI] = r;
            }).whenComplete((longIds, e) -> {
                if (e != null) {
                    log.error("Submit index job error: " + e.getMessage());
                    return;
                }

                firstOneSlot.submitIndexJobDone();
            });
        }

        SettablePromise<Reply> finalPromise = new SettablePromise<>();
        var asyncReply = new AsyncReply(finalPromise);

        Promises.all(promises).whenComplete((r, e) -> {
            if (e != null) {
                log.error("mget error: {}", e.getMessage());
                return;
            }

            TreeSet<Long> mergedLongIds = new TreeSet<>();
            if (isOr) {
                for (var getLongIds : returnSetArray) {
                    mergedLongIds.addAll(getLongIds);
                }
            } else {
                for (var getLongIds : returnSetArray) {
                    if (mergedLongIds.isEmpty()) {
                        mergedLongIds.addAll(getLongIds);
                    } else {
                        mergedLongIds.retainAll(getLongIds);
                    }
                }
            }

            if (mergedLongIds.isEmpty()) {
                finalPromise.set(MultiBulkReply.EMPTY);
                return;
            }

            // dispatch use MGroup mget
            var dd = new byte[mergedLongIds.size() + 1][];
            dd[0] = "mget".getBytes();
            int i = 1;
            for (var longId : mergedLongIds) {
                dd[i++] = (REVERSE_INDEX_DOC_KEY_PREFIX + longId).getBytes();
            }

            var mGroup = new MGroup("mget", dd, socket);
            mGroup.from(this);

            boolean isCrossRequestWorker = false;
            var slotWithKeyHashList = mGroup.parseSlots("mget", dd, slotNumber);
            if (slotWithKeyHashList != null && slotWithKeyHashList.size() > 1) {
                // check if cross threads
                int expectRequestWorkerId = -1;
                for (var slotWithKeyHash : slotWithKeyHashList) {
                    int slot = slotWithKeyHash.slot();
                    var expectRequestWorkerIdInner = slot % ConfForGlobal.netWorkers;
                    if (expectRequestWorkerId == -1) {
                        expectRequestWorkerId = expectRequestWorkerIdInner;
                    }
                    if (expectRequestWorkerId != expectRequestWorkerIdInner) {
                        isCrossRequestWorker = true;
                        break;
                    }
                }
            }

            mGroup.setCrossRequestWorker(isCrossRequestWorker);
            mGroup.setSlotWithKeyHashListParsed(slotWithKeyHashList);
            var first = slotWithKeyHashList.getFirst();
            var firstSlot = first.slot();
            int ii = firstSlot % ConfForGlobal.netWorkers;

            var currentThreadId = Thread.currentThread().threadId();
            var expectThreadId = MultiWorkerServer.STATIC_GLOBAL_V.netWorkerThreadIds[ii];
            if (currentThreadId == expectThreadId) {
                setReplyWhenAsyncMget(mGroup, isCrossRequestWorker, finalPromise);
                return;
            } else {
                var oneSlots = localPersist.oneSlots();
                for (var oneSlot : oneSlots) {
                    if (oneSlot.getThreadIdProtectedForSafe() == expectThreadId) {
                        boolean finalIsCrossRequestWorker = isCrossRequestWorker;
                        oneSlot.asyncRun(() -> {
                            setReplyWhenAsyncMget(mGroup, finalIsCrossRequestWorker, finalPromise);
                        });
                    }
                }
            }
        });

        return asyncReply;
    }

    private static void setReplyWhenAsyncMget(MGroup mGroup, boolean isCrossRequestWorker, SettablePromise<Reply> finalPromise) {
        var mgetReply = mGroup.mget();
        if (!isCrossRequestWorker) {
            finalPromise.set(mgetReply);
            return;
        }

        var mgetAsyncReply = (AsyncReply) mgetReply;
        mgetAsyncReply.getSettablePromise().whenComplete((r1, e1) -> {
            if (e1 != null) {
                log.error("mget error: {}", e1.getMessage());
                finalPromise.setException(e1);
                return;
            }

            finalPromise.set(r1);
        });
    }

    private Reply vv_add() {
        var slotWithKeyHashTmp = slotWithKeyHashListParsed.getFirst();
        var longId = slotWithKeyHashTmp.keyHash();

        var key = REVERSE_INDEX_DOC_KEY_PREFIX + longId;
        var keyBytes = key.getBytes();
        var slotWithKeyHash = new SlotWithKeyHash(slotWithKeyHashTmp.slot(), slotWithKeyHashTmp.bucketIndex(), KeyHash.hash(keyBytes));

        var valueBytes = data[2];
        set(keyBytes, valueBytes, slotWithKeyHash);

        TreeSet<String> words = new TreeSet<>();
        var value = new String(valueBytes);
        var wordsArray = value.split(",");
        for (var word : wordsArray) {
            // filter all alphabet words
            var wordBytes = word.getBytes();
            if (wordBytes.length == 0) {
                continue;
            }

            var isAllAlphabet = true;
            for (var wordByte : wordBytes) {
                if (wordByte < 'a' || wordByte > 'z') {
                    // A-Z case
                    if (wordByte < 'A' || wordByte > 'Z') {
                        isAllAlphabet = false;
                        break;
                    }
                }
            }
            if (!isAllAlphabet) {
                continue;
            }

            words.add(word);
        }

        if (words.isEmpty()) {
            return IntegerReply.REPLY_0;
        }

        var oneSlot = localPersist.oneSlot(slotWithKeyHash.slot());
        for (var word : words) {
            oneSlot.submitIndexJobRun(word, (indexHandler) -> {
                indexHandler.putWordIfNotExist(word);
                indexHandler.addLongId(word, longId);
            }).whenComplete((v, e) -> {
                if (e != null) {
                    log.error("Submit index job error: " + e.getMessage());
                    return;
                }

                oneSlot.submitIndexJobDone();
            });
        }

        return new IntegerReply(words.size());
    }
}
