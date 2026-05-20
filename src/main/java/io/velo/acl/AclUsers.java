package io.velo.acl;

import io.activej.eventloop.Eventloop;
import io.velo.ValkeyRawConfSupport;
import org.apache.commons.io.FileUtils;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Singleton managing user ACLs using a copy-on-write immutable snapshot.
 * All reads go through a single {@link AtomicReference} to an immutable map.
 * Writes create a new map and publish it atomically via CAS.
 * This eliminates the need for per-slot thread-local copies and async propagation.
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

    /**
     * Single globally-published immutable ACL snapshot.
     * Readers (net workers, slot workers, test threads) all read from this reference.
     * Writers create a new immutable map and CAS-publish it.
     */
    private final AtomicReference<Map<String, U>> usersRef = new AtomicReference<>(initialUsers());

    private static Map<String, U> initialUsers() {
        var map = new HashMap<String, U>();
        var defaultCopy = new U(U.DEFAULT_USER);
        defaultCopy.copyStateFrom(U.INIT_DEFAULT_U);
        defaultCopy.freeze();
        map.put(U.DEFAULT_USER, defaultCopy);
        return Map.copyOf(map);
    }

    /**
     * @return a copy of the current user list
     */
    public List<U> getUList() {
        return new ArrayList<>(usersRef.get().values());
    }

    /**
     * @param user the username to look up
     * @return the user object if found, otherwise null
     */
    public U get(String user) {
        return usersRef.get().get(user);
    }

    /**
     * Reads the ACL snapshot for a given user. Equivalent to {@link #get(String)}.
     *
     * @param user the username to look up
     * @return a snapshot copy of the user, or null if not found
     */
    public U getSnapshotUser(String user) {
        return usersRef.get().get(user);
    }

    /**
     * @param slotWorkerEventloopArray the array of Eventloop instances (kept for API compat, no longer used)
     */
    public void initBySlotWorkerEventloopArray(Eventloop[] slotWorkerEventloopArray) {
        log.info("Acl users init by slot worker eventloop array (no-op, using global snapshot)");
    }

    /** Initializes for testing with a fresh default snapshot. */
    @TestOnly
    public void initForTest() {
        usersRef.set(initialUsers());
    }

    /** Resets for testing. */
    @TestOnly
    public void resetForTest() {
        usersRef.set(initialUsers());
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
     * Updates or inserts a user using copy-on-write.
     *
     * @param user     the username to update or insert
     * @param callback the callback to perform the update operation
     * @throws AclInvalidRuleException if the update callback fails
     */
    public void upInsert(String user, UpdateCallback<U> callback) {
        while (true) {
            var oldMap = usersRef.get();
            var newMap = new HashMap<String, U>(oldMap);

            var existing = newMap.get(user);
            var target = existing != null ? existing.deepCopy() : new U(user);
            callback.doUpdate(target);
            target.freeze();
            newMap.put(user, target);

            if (usersRef.compareAndSet(oldMap, Map.copyOf(newMap))) {
                return;
            }
        }
    }

    /**
     * Deletes a user using copy-on-write.
     *
     * @param user the username to delete
     * @return true if the user was deleted
     */
    public boolean delete(String user) {
        while (true) {
            var oldMap = usersRef.get();
            if (!oldMap.containsKey(user)) {
                return false;
            }

            var newMap = new HashMap<String, U>(oldMap);
            newMap.remove(user);

            if (usersRef.compareAndSet(oldMap, Map.copyOf(newMap))) {
                return true;
            }
        }
    }

    /**
     * Replaces all users using copy-on-write.
     *
     * @param uList the new list of user objects
     */
    public void replaceUsers(List<U> uList) {
        while (true) {
            var oldMap = usersRef.get();
            var newMap = new HashMap<String, U>();
            for (var u : uList) {
                var copy = u.deepCopy();
                copy.freeze();
                newMap.put(u.getUser(), copy);
            }
            // always ensure default user exists
            if (!newMap.containsKey(U.DEFAULT_USER)) {
                var defaultCopy = new U(U.DEFAULT_USER);
                defaultCopy.copyStateFrom(U.INIT_DEFAULT_U);
                defaultCopy.freeze();
                newMap.put(U.DEFAULT_USER, defaultCopy);
            }

            if (usersRef.compareAndSet(oldMap, Map.copyOf(newMap))) {
                return;
            }
        }
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
