package io.velo;

/**
 * Mark those class object required resources need to be cleaned up.
 */
public interface NeedCleanUp {
    /** Releases any resources held by this object. */
    void cleanUp();
}
