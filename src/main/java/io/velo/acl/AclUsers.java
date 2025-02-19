package io.velo.acl;

import io.activej.eventloop.Eventloop;
import io.velo.ThreadNeedLocal;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

/**
 * Singleton class managing user access control lists (ACLs) in a multi-threaded environment.
 * Each inner class instance handles ACL operations in the context of a specific thread.
 */
public class AclUsers {
    // singleton instance
    private static final AclUsers instance = new AclUsers();

    /**
     * Returns the singleton instance of AclUsers.
     *
     * @return The singleton AclUsers instance.
     */
    public static AclUsers getInstance() {
        return instance;
    }

    private AclUsers() {
    }

    /**
     * Callback interface for updating user objects within the ACL.
     *
     * @param <U> The type of user object.
     */
    public interface UpdateCallback<U> {
        void doUpdate(U u) throws AclInvalidRuleException;
    }

    private static final Logger log = LoggerFactory.getLogger(AclUsers.class);

    /**
     * Array of event loops, each associated with a network worker thread.
     */
    @ThreadNeedLocal
    private Eventloop[] netWorkerEventloopArray;

    /**
     * Array of inner instances, each corresponding to a specific thread.
     */
    @ThreadNeedLocal
    private Inner[] inners;

    /**
     * Initializes the AclUsers instance with event loops associated with network worker threads.
     *
     * @param netWorkerEventloopArray Array of Eventloop instances.
     */
    public void initByNetWorkerEventloopArray(Eventloop[] netWorkerEventloopArray) {
        this.netWorkerEventloopArray = netWorkerEventloopArray;

        inners = new Inner[netWorkerEventloopArray.length];
        for (int i = 0; i < netWorkerEventloopArray.length; i++) {
            var eventloop = netWorkerEventloopArray[i];
            var eventloopThread = eventloop.getEventloopThread();
            inners[i] = new Inner(eventloopThread != null ? eventloopThread.threadId() : Thread.currentThread().threadId());
        }
        log.info("Acl users init by net worker eventloop array");
    }

    /**
     * Initializes the AclUsers instance for testing purposes with a single inner instance.
     */
    @TestOnly
    public void initForTest() {
        inners = new Inner[1];
        inners[0] = new Inner(Thread.currentThread().threadId());
    }

    /**
     * Inner class handling user operations within the context of a specific thread.
     * Each Inner instance is associated with a specific thread ID.
     */
    @ThreadNeedLocal
    public static class Inner {
        /**
         * Constructs an Inner instance for a given thread ID.
         * Initializes users list with a default user.
         *
         * @param expectThreadId The thread ID this inner instance is expected to handle.
         */
        public Inner(long expectThreadId) {
            this.expectThreadId = expectThreadId;
            users.add(U.INIT_DEFAULT_U); // Add a default user to the list.
        }

        final long expectThreadId;

        /**
         * List of user objects managed by this inner instance.
         */
        private final List<U> users = new ArrayList<>();

        /**
         * Returns a copy of the current list of users.
         *
         * @return An immutable list of users.
         */
        public List<U> getUsers() {
            return new ArrayList<>(users);
        }

        /**
         * Retrieves a user by username.
         *
         * @param user The username to look up.
         * @return The user object if found; otherwise null.
         */
        public U get(String user) {
            return users.stream().filter(u -> u.user.equals(user)).findFirst().orElse(null);
        }

        /**
         * Updates or inserts a user into the list based on the provided username.
         * If the user already exists, the UpdateCallback is called with the existing user object.
         * If the user does not exist, a new user is created, the callback is called, and the user is added to the list.
         *
         * @param user     The username to update or insert.
         * @param callback The callback to perform the update operation.
         */
        public void upInsert(String user, UpdateCallback<U> callback) {
            var one = users.stream().filter(u1 -> u1.user.equals(user)).findFirst();
            if (one.isPresent()) {
                callback.doUpdate(one.get());
            } else {
                var u = new U(user);
                callback.doUpdate(u);
                users.add(u);
            }
        }

        /**
         * Deletes a user from the list based on the provided username.
         *
         * @param user The username to delete.
         * @return true if the user was successfully deleted; otherwise false.
         */
        public boolean delete(String user) {
            return users.removeIf(u -> u.user.equals(user));
        }
    }

    /**
     * Returns the Inner instance associated with the current thread.
     *
     * @return The Inner instance or null if no matching instance is found.
     */
    public Inner getInner() {
        var currentThreadId = Thread.currentThread().threadId();
        for (var inner : inners) {
            if (inner.expectThreadId == currentThreadId) {
                return inner;
            }
        }
        return null;
    }

    /**
     * Retrieves a user by username.
     *
     * @param user The username to look up.
     * @return The user object if found; otherwise null.
     */
    public U get(String user) {
        var inner = getInner();
        return inner == null ? null : inner.get(user);
    }

    /**
     * Functional interface for operations to be performed on a specific inner instance.
     */
    private interface DoInTargetEventloop {
        void doSth(Inner inner);
    }

    /**
     * Executes a given operation on the inner instance associated with the current thread.
     * If the inner instance is not associated with the current thread, the operation is scheduled to run on the target event loop.
     *
     * @param doInTargetEventloop The operation to perform.
     */
    private void changeUser(DoInTargetEventloop doInTargetEventloop) {
        var currentThreadId = Thread.currentThread().threadId();
        for (int i = 0; i < inners.length; i++) {
            var inner = inners[i];
            if (inner.expectThreadId == currentThreadId) {
                doInTargetEventloop.doSth(inner);
            } else {
                var targetEventloop = netWorkerEventloopArray[i];
                targetEventloop.execute(() -> {
                    doInTargetEventloop.doSth(inner);
                });
            }
        }
    }

    /**
     * Updates or inserts a user across all Inner instances.
     *
     * @param user     The username to update or insert.
     * @param callback The callback to perform the update operation.
     */
    public void upInsert(String user, UpdateCallback<U> callback) {
        var inner = getInner();
        inner.upInsert(user, callback);

        changeUser(inner2 -> inner2.upInsert(user, callback));
    }

    /**
     * Deletes a user from all Inner instances.
     *
     * @param user The username to delete.
     * @return true if the user was successfully deleted from any Inner instance; otherwise false.
     */
    public boolean delete(String user) {
        var inner = getInner();
        var flag = inner.delete(user);

        changeUser(inner2 -> {
            inner2.delete(user);
        });
        return flag;
    }

    /**
     * Replaces the list of users in all Inner instances with a new list of users.
     *
     * @param users The new list of user objects.
     */
    public void replaceUsers(List<U> users) {
        var inner = getInner();
        inner.users.clear();
        inner.users.addAll(users);

        changeUser(inner2 -> {
            inner2.users.clear();
            inner2.users.addAll(users);
        });
    }
}