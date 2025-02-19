package io.velo;

/**
 * Mark those class object required resources need to be cleaned up.
 */
public interface NeedCleanUp {
    void cleanUp();
}
