package io.velo.acl;

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

    private Inner[] inners;

    public void initByNetWorkerThreadIds(long[] netWorkerThreadIds) {
        inners = new Inner[netWorkerThreadIds.length];
        for (int i = 0; i < netWorkerThreadIds.length; i++) {
            inners[i] = new Inner(netWorkerThreadIds[i]);
        }
        log.info("Acl users init by net worker thread ids: {}", netWorkerThreadIds);
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

    private Inner getInner() {
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

    public void upInsert(String user, UpdateCallback callback) {
        for (var inner : inners) {
            inner.upInsert(user, callback);
        }
    }

    public boolean delete(String user) {
        boolean flag = false;
        for (var inner : inners) {
            if (inner.delete(user)) {
                flag = true;
            }
        }
        return flag;
    }
}
