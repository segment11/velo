package io.velo.e2e

import spock.lang.Specification

class TestUtil extends Specification {
    static boolean waitUntil(int retryCount = 20, long sleepMillis = 1_000L, Closure<Boolean> condition) {
        for (int i = 0; i < retryCount; i++) {
            if (condition.call()) {
                return true
            }
            Thread.sleep(sleepMillis)
        }
        return false
    }
}
