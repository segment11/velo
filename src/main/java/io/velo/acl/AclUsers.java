package io.velo.acl;

import io.activej.eventloop.Eventloop;
import io.velo.ThreadNeedLocal;
import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;

@ThreadNeedLocal
public class AclUsers {
    // singleton
    private static final AclUsers instance = new AclUsers();

    public static AclUsers getInstance() {
        return instance;
    }

    private AclUsers() {
    }

    public interface UpdateCallback {
        void doUpdate(U u);
    }

    private static final Logger log = LoggerFactory.getLogger(AclUsers.class);

    private Eventloop[] netWorkerEventloopArray;
    private Inner[] inners;

    public void initByNetWorkerEventloopArray(Eventloop[] netWorkerEventloopArray) {
        this.netWorkerEventloopArray = netWorkerEventloopArray;

        inners = new Inner[netWorkerEventloopArray.length];
        for (int i = 0; i < netWorkerEventloopArray.length; i++) {
            var eventloop = netWorkerEventloopArray[i];
            inners[i] = new Inner(eventloop.getEventloopThread().threadId());
        }
        log.info("Acl users init by net worker eventloop array");
    }

    @TestOnly
    public void initForTest() {
        inners = new Inner[1];
        inners[0] = new Inner(Thread.currentThread().threadId());
    }

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
        changeUser(inner -> inner.upInsert(user, callback));
    }

    public boolean delete(String user) {
        var flag = getInner().delete(user);

        changeUser(inner -> {
            inner.delete(user);
        });
        return flag;
    }

    public void replaceUsers(List<U> users) {
        changeUser(inner -> {
            inner.users.clear();
            inner.users.addAll(users);
        });
    }
}
