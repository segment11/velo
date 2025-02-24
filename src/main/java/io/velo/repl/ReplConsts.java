package io.velo.repl;

/**
 * A class containing constants used in the Velo REPL system.
 */
public class ReplConsts {
    /**
     * For publish message after master reset, velo server act as a sentinel server.
     * Velo now support redis sentinel server, do not need this anymore.
     */
    @Deprecated
    public static final String REPL_MASTER_NAME_READONLY_SLAVE_SUFFIX = "/readonly_slave";
}
