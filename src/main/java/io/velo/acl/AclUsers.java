package io.velo.acl;

import io.activej.eventloop.Eventloop;
import io.velo.ThreadNeedLocal;
import io.velo.ValkeyRawConfSupport;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Singleton managing user ACLs in a multi-threaded environment.
 */
public class AclUsers {
    private static final AclUsers instance = new AclUsers();

    /** @return the singleton instance */
    public static AclUsers getInstance() {
        return instance;
    }

    private AclUsers() {
    }

    /**
     * Callback interface for ACL user updates.
     * @param <U> the type of user object
     */
    public interface UpdateCallback<U> {
        void doUpdate(U u) throws AclInvalidRuleException;
    }

    private static final Logger log = LoggerFactory.getLogger(AclUsers.class);

    /** Array of event loops, each associated with a network worker thread. */
    @ThreadNeedLocal
    private Eventloop[] slotWorkerEventloopArray;

    /** Array of inner instances, each corresponding to a specific thread. */
    @ThreadNeedLocal
    private Inner[] inners;

    private Inner preInitInner;

    /**
     * @param slotWorkerEventloopArray the array of Eventloop instances
     */
    public void initBySlotWorkerEventloopArray(Eventloop[] slotWorkerEventloopArray) {
        this.slotWorkerEventloopArray = slotWorkerEventloopArray;

        inners = new Inner[slotWorkerEventloopArray.length];
        for (int i = 0; i < slotWorkerEventloopArray.length; i++) {
            var eventloop = slotWorkerEventloopArray[i];
            var eventloopThread = eventloop.getEventloopThread();
            inners[i] = new Inner(eventloopThread != null ? eventloopThread.threadId() : Thread.currentThread().threadId());
        }

        if (preInitInner != null) {
            for (var inner : inners) {
                inner.uList.clear();
                inner.uList.addAll(preInitInner.uList);
            }
            preInitInner = null;
        }
        log.info("Acl users init by slot worker eventloop array");
    }

    /** Initializes for testing with a single inner instance. */
    @TestOnly
    public void initForTest() {
        inners = new Inner[1];
        inners[0] = new Inner(Thread.currentThread().threadId());

        if (preInitInner != null) {
            inners[0].uList.clear();
            inners[0].uList.addAll(preInitInner.uList);
            preInitInner = null;
        }
    }

    /** Resets for testing. */
    @TestOnly
    public void resetForTest() {
        inners = null;
        preInitInner = null;
    }

    /** @return the Inner instance for the current thread, or null if not found */
    public Inner getInner() {
        if (inners == null) {
            if (preInitInner == null) {
                preInitInner = new Inner(Thread.currentThread().threadId());
            }
            return preInitInner;
        }
        var currentThreadId = Thread.currentThread().threadId();
        for (var inner : inners) {
            if (inner.expectThreadId == currentThreadId) {
                return inner;
            }
        }
        // when run in networker thread, return the first inner instance, only for read
        return inners[0];
    }

    /**
     * Returns the Inner instance owned by the current thread only (no fallback).
     * Returns null if no inner is owned by the current thread or if not initialized.
     * Use this for write operations to avoid mutating another thread's inner.
     */
    private Inner getOwnedInner() {
        if (inners == null) {
            if (preInitInner == null) {
                preInitInner = new Inner(Thread.currentThread().threadId());
            }
            return preInitInner;
        }
        var currentThreadId = Thread.currentThread().threadId();
        for (var inner : inners) {
            if (inner.expectThreadId == currentThreadId) {
                return inner;
            }
        }
        return null;
    }

    /** Loads the ACL file and replaces the existing user list. */
    public void loadAclFile() {
        var aclFile = Paths.get(ValkeyRawConfSupport.aclFilename).toFile();
        if (!aclFile.exists()) {
            return;
        }

        List<U> tmpUList = new ArrayList<>();
        try {
            var lines = FileUtils.readLines(aclFile, "UTF-8");
            for (var line : lines) {
                if (line.startsWith("#")) {
                    continue;
                }

                var u = U.fromLiteral(line);
                if (u == null) {
                    throw new IllegalArgumentException("parse acl file error: " + line);
                }
                tmpUList.add(u);
            }
        } catch (IOException e) {
            log.error("Read acl file error={}", e.getMessage());
            throw new RuntimeException("read acl file error: " + e.getMessage());
        } catch (IllegalArgumentException e) {
            log.error("Parse acl file error={}", e.getMessage());
            throw new RuntimeException("parse acl file error: " + e.getMessage());
        }

        // must include default user
        if (tmpUList.stream().noneMatch(u -> u.getUser().equals(U.INIT_DEFAULT_U.getUser()))) {
            log.error("no default user in acl file");
            throw new IllegalArgumentException("no default user in acl file");
        }

        replaceUsers(tmpUList);
    }

    /**
     * Inner class handling user operations within a specific thread.
     */
    @ThreadNeedLocal
    public static class Inner {
        /**
         * @param expectThreadId the thread ID this inner instance is expected to handle
         */
        Inner(long expectThreadId) {
            this.expectThreadId = expectThreadId;
            var defaultCopy = new U(U.DEFAULT_USER);
            defaultCopy.copyStateFrom(U.INIT_DEFAULT_U);
            uList.add(defaultCopy);
        }

        final long expectThreadId;

        /** List of user objects managed by this inner instance. */
        private final List<U> uList = new ArrayList<>();

        /** @return a copy of the current list of users */
        public List<U> getUList() {
            return new ArrayList<>(uList);
        }

        /**
         * @param user the username to look up
         * @return the user object if found, otherwise null
         */
        public U get(String user) {
            return uList.stream().filter(u -> u.user.equals(user)).findFirst().orElse(null);
        }

        /**
         * Updates or inserts a user.
         * @param user     the username to update or insert
         * @param callback the callback to perform the update operation
         */
        public void upInsert(String user, UpdateCallback<U> callback) {
            var one = uList.stream().filter(u1 -> u1.user.equals(user)).findFirst();
            if (one.isPresent()) {
                callback.doUpdate(one.get());
            } else {
                var u = new U(user);
                callback.doUpdate(u);
                uList.add(u);
            }
        }

        /**
         * @param user the username to delete
         * @return true if the user was deleted
         */
        public boolean delete(String user) {
            return uList.removeIf(u -> u.user.equals(user));
        }
    }

    /**
     * @param user the username to look up
     * @return the user object if found, otherwise null
     */
    public U get(String user) {
        var inner = getInner();
        return inner == null ? null : inner.get(user);
    }

    /** Functional interface for operations on a specific inner instance. */
    private interface DoInTargetEventloop {
        void doSth(Inner inner);
    }

    /**
     * @param doInTargetEventloop the operation to perform
     */
    private void changeUser(DoInTargetEventloop doInTargetEventloop) {
        if (inners == null) {
            return;
        }
        var currentThreadId = Thread.currentThread().threadId();
        for (int i = 0; i < inners.length; i++) {
            var inner = inners[i];
            if (inner.expectThreadId == currentThreadId) {
                // skip current thread's inner; caller already applied the mutation
                continue;
            }
            var targetEventloop = slotWorkerEventloopArray[i];
            targetEventloop.execute(() -> {
                doInTargetEventloop.doSth(inner);
            });
        }
    }

    /**
     * @param user     the username to update or insert
     * @param callback the callback to perform the update operation
     * @throws AclInvalidRuleException if the update callback fails
     */
    public void upInsert(String user, UpdateCallback<U> callback) {
        var inner = getOwnedInner();
        if (inner != null) {
            inner.upInsert(user, callback);
        }

        changeUser(inner2 -> inner2.upInsert(user, callback));
    }

    /**
     * @param user the username to delete
     * @return true if the user was deleted from any Inner instance
     */
    public boolean delete(String user) {
        var inner = getOwnedInner();
        boolean flag;
        if (inner != null) {
            flag = inner.delete(user);
        } else {
            flag = getInner().get(user) != null;
        }

        changeUser(inner2 -> {
            inner2.delete(user);
        });
        return flag;
    }

    /**
     * @param uList the new list of user objects
     */
    public void replaceUsers(List<U> uList) {
        var inner = getOwnedInner();
        if (inner != null) {
            inner.uList.clear();
            inner.uList.addAll(uList);
        }

        changeUser(inner2 -> {
            inner2.uList.clear();
            inner2.uList.addAll(uList);
        });
    }

    private static final int ACL_LOG_MAX_SIZE = 128;
    private static final LogRow[] aclLogBuffer = new LogRow[ACL_LOG_MAX_SIZE];
    private static int aclLogIndex = 0;
    private static int aclLogCount = 0;
    private static long aclLogEntryIdCounter = 1;

    /**
     * @param reason    the ACL log reason
     * @param context   the ACL log context
     * @param object    the ACL log object
     * @param username  the username
     * @param clientInfo the client info
     */
    public static synchronized void recordAclLog(String reason, String context, String object, String username, String clientInfo) {
        var entry = new LogRow();
        entry.count = aclLogCount;
        entry.reason = reason;
        entry.context = context;
        entry.object = object;
        entry.username = username;
        entry.ageSeconds = 0.0;
        entry.clientInfo = clientInfo;
        entry.entryId = aclLogEntryIdCounter++;
        entry.timestampCreated = System.currentTimeMillis();
        entry.timestampLastUpdated = entry.timestampCreated;

        aclLogBuffer[aclLogIndex % ACL_LOG_MAX_SIZE] = entry;
        aclLogIndex++;
        if (aclLogCount < ACL_LOG_MAX_SIZE) {
            aclLogCount++;
        }
    }

    /**
     * @param count the maximum number of log entries to return
     * @return array of ACL log entries
     */
    public static synchronized LogRow[] getAclLogs(int count) {
        int size = Math.min(count, aclLogCount);
        if (size == 0) {
            return new LogRow[0];
        }
        var result = new LogRow[size];
        int startIndex = aclLogIndex - aclLogCount;
        long now = System.currentTimeMillis();
        for (int i = 0; i < size; i++) {
            int bufferIndex = (startIndex + i) % ACL_LOG_MAX_SIZE;
            var entry = aclLogBuffer[bufferIndex];
            if (entry != null) {
                var copy = new LogRow();
                copy.count = entry.count;
                copy.reason = entry.reason;
                copy.context = entry.context;
                copy.object = entry.object;
                copy.username = entry.username;
                copy.ageSeconds = (now - entry.timestampCreated) / 1000.0;
                copy.clientInfo = entry.clientInfo;
                copy.entryId = entry.entryId;
                copy.timestampCreated = entry.timestampCreated;
                copy.timestampLastUpdated = entry.timestampLastUpdated;
                result[i] = copy;
            }
        }
        return result;
    }

    /** Resets all ACL log entries. */
    public static synchronized void resetAclLogs() {
        Arrays.fill(aclLogBuffer, null);
        aclLogIndex = 0;
        aclLogCount = 0;
    }
}