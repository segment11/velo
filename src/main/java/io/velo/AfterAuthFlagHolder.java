package io.velo;

import java.net.InetSocketAddress;
import java.util.concurrent.ConcurrentHashMap;

public class AfterAuthFlagHolder {
    private AfterAuthFlagHolder() {
    }

    // need thread safe
    private static final ConcurrentHashMap<InetSocketAddress, Boolean> flagBySocketAddress = new ConcurrentHashMap<>();

    public static void add(InetSocketAddress address) {
        flagBySocketAddress.put(address, true);
    }

    public static boolean contains(InetSocketAddress address) {
        return flagBySocketAddress.containsKey(address);
    }

    public static void remove(InetSocketAddress address) {
        flagBySocketAddress.remove(address);
    }
}
