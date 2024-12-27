package io.velo.acl;

import io.activej.eventloop.Eventloop;
import io.velo.ThreadNeedLocal;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

public class AclUsers {
    // singleton
    private static final AclUsers instance = new AclUsers();

    public static AclUsers getInstance() {
        return instance;
    }

    private AclUsers() {
    }

    public interface UpdateCallback {
        void doUpdate(U u) throws AclInvalidRuleException;
    }

    private static final Logger log = LoggerFactory.getLogger(AclUsers.class);

    @ThreadNeedLocal
    private Eventloop[] netWorkerEventloopArray;
    @ThreadNeedLocal
    private Inner[] inners;

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

    @TestOnly
    public void initForTest() {
        inners = new Inner[1];
        inners[0] = new Inner(Thread.currentThread().threadId());
    }

    @ThreadNeedLocal
    public static class Inner {
        public Inner(long expectThreadId) {
            this.expectThreadId = expectThreadId;
            users.add(U.INIT_DEFAULT_U);
        }

        final long expectThreadId;

        private final List<U> users = new ArrayList<>();

        // immutable
        public List<U> getUsers() {
            // copy better
            return users;
        }

        public U get(String user) {
            return users.stream().filter(u -> u.user.equals(user)).findFirst().orElse(null);
        }

        public void upInsert(String user, UpdateCallback callback) {
            var one = users.stream().filter(u1 -> u1.user.equals(user)).findFirst();
            if (one.isPresent()) {
                callback.doUpdate(one.get());
            } else {
                var u = new U(user);
                callback.doUpdate(u);
                users.add(u);
            }
        }

        public boolean delete(String user) {
            return users.removeIf(u -> u.user.equals(user));
        }
    }

    public Inner getInner() {
        var currentThreadId = Thread.currentThread().threadId();
        for (var inner : inners) {
            if (inner.expectThreadId == currentThreadId) {
                return inner;
            }
        }
        return null;
    }

    public U get(String user) {
        var inner = getInner();
        return inner == null ? null : inner.get(user);
    }

    private interface DoInTargetEventloop {
        void doSth(Inner inner);
    }

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

    public void upInsert(String user, UpdateCallback callback) {
        var inner = getInner();
        inner.upInsert(user, callback);

        changeUser(inner2 -> inner2.upInsert(user, callback));
    }

    public boolean delete(String user) {
        var inner = getInner();
        var flag = inner.delete(user);

        changeUser(inner2 -> {
            inner2.delete(user);
        });
        return flag;
    }

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
