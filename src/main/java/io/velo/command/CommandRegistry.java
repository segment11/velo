package io.velo.command;

import org.jetbrains.annotations.TestOnly;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Process-wide registry of every Redis-compatible command Velo implements.
 *
 * <p>Populated once during startup by the {@code static {}} blocks of the
 * {@code AGroup}–{@code ZGroup} command classes, then read (lock-free) by the
 * {@code COMMAND} subcommands implemented in {@link CommandCommand}.
 *
 * <h2>Registration contract</h2>
 * {@link #register(CommandEntry)} must be called <em>only</em> from a command
 * group's static initializer. Group classes are loaded during the serial
 * per-worker bootstrap in {@code MultiWorkerServer} (the
 * {@code RequestHandler} construction loop), which runs exactly once under the
 * JVM class-init lock and completes before any client request is served. The
 * registry must not be mutated after startup. No synchronization is required
 * because registration is confined to that single static-init phase.
 *
 * <p>Lookups ({@link #get(String)}) are case-insensitive to match Redis command
 * name resolution.
 */
public final class CommandRegistry {
    private static final Logger log = LoggerFactory.getLogger(CommandRegistry.class);

    /**
     * Insertion-ordered map keyed by lowercased command name.
     * {@code COMMAND LIST} documents {@code NONDETERMINISTIC_OUTPUT_ORDER}, so
     * insertion order (rather than alphabetical) is acceptable.
     */
    private static final Map<String, CommandEntry> ALL = new LinkedHashMap<>();

    private CommandRegistry() {
    }

    /**
     * Registers one command. If a command with the same (case-insensitive) name
     * is already registered, logs a warning and keeps the first registration
     * (does not overwrite), so accidental double-registration is safe.
     *
     * @param entry the command metadata; its {@code name} is lowercased for the key
     */
    public static void register(CommandEntry entry) {
        var key = entry.name().toLowerCase();
        if (ALL.containsKey(key)) {
            log.warn("Command already registered, skip duplicate name={}, keep first", key);
            return;
        }
        ALL.put(key, entry);
    }

    /**
     * Looks up a command by name (case-insensitive).
     *
     * @param name the command name in any case
     * @return the entry, or {@code null} if not registered
     */
    public static CommandEntry get(String name) {
        if (name == null) {
            return null;
        }
        return ALL.get(name.toLowerCase());
    }

    /**
     * Returns a snapshot of all registered entries in insertion order.
     *
     * <p>The returned collection is a defensive copy, so it is immune to any
     * later mutation of the registry.
     *
     * @return a new list of all command entries
     */
    public static Collection<CommandEntry> all() {
        return new ArrayList<>(ALL.values());
    }

    /**
     * Returns the number of registered commands.
     *
     * @return the count
     */
    public static int size() {
        return ALL.size();
    }

    /**
     * Clears the registry. Test only — production code never clears the registry.
     *
     * <p><b>Hazard:</b> group class static initializers run only once per JVM, so
     * calling this does NOT cause commands to be re-registered. Any caller that
     * clears must restore the prior state (e.g. via {@link #all()} snapshot +
     * {@link #register}) so a shared-JVM test run does not observe an empty
     * registry afterwards.
     */
    @TestOnly
    public static void clear() {
        ALL.clear();
    }
}
