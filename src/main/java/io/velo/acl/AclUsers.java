package io.velo.acl;

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

    public static interface UpdateCallback {
        void doUpdate(U u);
    }

    private final List<U> users = new ArrayList<>();

    public void upInsert(U u, UpdateCallback callback) {
        var user = users.stream().filter(u1 -> u1.user.equals(u.user)).findFirst();
        if (user.isPresent()) {
            callback.doUpdate(user.get());
        } else {
            users.add(u);
            callback.doUpdate(u);
        }
    }
}
