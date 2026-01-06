package io.velo;

/**
 * A singleton class that provides debug switches and flags for controlling various debug behaviors in the application.
 * These flags can be toggled to enable or disable specific debug logging and features during development and testing.
 */
public class Debug {
    /**
     * Singleton instance of the Debug class.
     */
    private static final Debug instance = new Debug();

    /**
     * Private constructor to prevent instantiation from outside the class.
     */
    private Debug() {
    }

    /**
     * Returns the singleton instance of the Debug class.
     *
     * @return the Debug singleton instance
     */
    public static Debug getInstance() {
        return instance;
    }

    /**
     * Flag to enable or disable command logging.
     * When {@code true}, commands are logged for debugging purposes.
     */
    public boolean logCmd = false;

    /**
     * Flag to enable or disable dictionary training logging.
     * When {@code true}, dictionary training operations are logged.
     */
    public boolean logTrainDict = false;

    /**
     * Flag to enable or disable restoration logging.
     * When {@code true}, restoration operations are logged.
     */
    public boolean logRestore = false;

    /**
     * Flag to enable or disable bulk load mode.
     * When {@code true}, the system operates in bulk load mode for optimized data ingestion.
     */
    public boolean bulkLoad = false;
}
