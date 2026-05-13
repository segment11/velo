package io.velo.repl;

/**
 * A class containing constants used in the Velo REPL system.
 */
public class ReplConsts {
    /**
     * @deprecated Velo now supports Redis sentinel server, this is no longer needed.
     */
    @Deprecated
    public static final String REPL_MASTER_NAME_READONLY_SLAVE_SUFFIX = "/readonly_slave";
}
