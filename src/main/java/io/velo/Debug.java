package io.velo;

/**
 * For debug, some switch
 */
public class Debug {
    // singleton
    private static final Debug instance = new Debug();

    private Debug() {
    }

    public static Debug getInstance() {
        return instance;
    }

    public boolean logCmd = false;

    public boolean logTrainDict = false;

    public boolean logRestore = false;

    public boolean bulkLoad = false;
}
