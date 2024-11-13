package io.velo.acl;

import java.util.ArrayList;
import java.util.List;

// todo, change to thread local
public class AclUsers {
    // singleton
    private static final AclUsers instance = new AclUsers();

    public static AclUsers getInstance() {
        return instance;
    }

    private AclUsers() {
        users.add(U.INIT_DEFAULT_U);
    }

    public interface UpdateCallback {
        void doUpdate(U u);
    }

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
