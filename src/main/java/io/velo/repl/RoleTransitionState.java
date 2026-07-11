package io.velo.repl;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Durable node-level state for a multi-slot replication role transition.
 *
 * @param epoch monotonically increasing transition identifier
 * @param targetRole role to which every initialized slot must converge
 * @param phase current transition phase
 * @param upstreamHost target upstream host for demotion, or an empty string for promotion
 * @param upstreamPort target upstream port for demotion, or zero for promotion
 * @param binlogTargets immutable promotion rebase position by slot
 * @param preparedSlots slots durably recorded as prepared
 * @param committedSlots slots durably recorded as committed
 * @param error last transition error, or null when no error has been recorded
 */
public record RoleTransitionState(long epoch,
                                  @NotNull TargetRole targetRole,
                                  @NotNull Phase phase,
                                  @NotNull String upstreamHost,
                                  int upstreamPort,
                                  @NotNull Map<Short, BinlogTarget> binlogTargets,
                                  @NotNull Set<Short> preparedSlots,
                                  @NotNull Set<Short> committedSlots,
                                  @Nullable String error) {
    public RoleTransitionState {
        binlogTargets = Map.copyOf(binlogTargets);
        preparedSlots = Set.copyOf(preparedSlots);
        committedSlots = Set.copyOf(committedSlots);
    }

    /** Target role shared by every slot in one transition epoch. */
    public enum TargetRole {
        MASTER,
        SLAVE
    }

    /** Durable phase used to resume or fence an interrupted transition. */
    public enum Phase {
        PREPARING,
        PREPARED,
        COMMITTING,
        STABLE,
        ABORTED,
        FAILED_FENCED
    }

    /**
     * Immutable promotion rebase position captured when the transition epoch begins.
     *
     * @param fileIndex binlog file index
     * @param offset binlog byte offset
     */
    public record BinlogTarget(int fileIndex, long offset) {
    }

    /**
     * Creates a transition in the {@link Phase#PREPARING} phase.
     *
     * @param epoch transition identifier
     * @param targetRole desired node role
     * @param upstreamHost upstream host for demotion
     * @param upstreamPort upstream port for demotion
     * @param binlogTargets immutable promotion target by slot
     * @return the initial transition state
     */
    public static @NotNull RoleTransitionState begin(long epoch,
                                                      @NotNull TargetRole targetRole,
                                                      @NotNull String upstreamHost,
                                                      int upstreamPort,
                                                      @NotNull Map<Short, BinlogTarget> binlogTargets) {
        return new RoleTransitionState(epoch, targetRole, Phase.PREPARING, upstreamHost, upstreamPort,
                binlogTargets, Set.of(), Set.of(), null);
    }

    /**
     * @param nextPhase replacement phase
     * @return a copy with the replacement phase
     */
    public @NotNull RoleTransitionState withPhase(@NotNull Phase nextPhase) {
        return new RoleTransitionState(epoch, targetRole, nextPhase, upstreamHost, upstreamPort,
                binlogTargets, preparedSlots, committedSlots, error);
    }

    /**
     * @param slot prepared slot
     * @return a copy recording the prepared slot
     */
    public @NotNull RoleTransitionState withPreparedSlot(short slot) {
        var updated = new HashSet<>(preparedSlots);
        updated.add(slot);
        return new RoleTransitionState(epoch, targetRole, phase, upstreamHost, upstreamPort,
                binlogTargets, updated, committedSlots, error);
    }

    /**
     * @param slot committed slot
     * @return a copy recording the committed slot
     */
    public @NotNull RoleTransitionState withCommittedSlot(short slot) {
        var updated = new HashSet<>(committedSlots);
        updated.add(slot);
        return new RoleTransitionState(epoch, targetRole, phase, upstreamHost, upstreamPort,
                binlogTargets, preparedSlots, updated, error);
    }

    /**
     * @param failedPhase failure phase to persist
     * @param message error description
     * @return a copy containing the failure
     */
    public @NotNull RoleTransitionState withError(@NotNull Phase failedPhase, @NotNull String message) {
        return new RoleTransitionState(epoch, targetRole, failedPhase, upstreamHost, upstreamPort,
                binlogTargets, preparedSlots, committedSlots, message);
    }

    /** Atomic JSON store rooted in the persistence directory. */
    public static final class Store {
        /** Durable transition-state file name. */
        public static final String FILE_NAME = "role-transition.json";
        private static final String TEMP_FILE_NAME = FILE_NAME + ".tmp";

        private final File stateFile;
        private final File tempFile;
        private final ObjectMapper objectMapper = new ObjectMapper();

        /**
         * @param persistDir node persistence directory
         */
        public Store(@NotNull File persistDir) {
            stateFile = new File(persistDir, FILE_NAME);
            tempFile = new File(persistDir, TEMP_FILE_NAME);
        }

        /**
         * Atomically replaces the durable transition state.
         *
         * @param state state to persist
         * @throws IOException if serialization or replacement fails
         */
        public void save(@NotNull RoleTransitionState state) throws IOException {
            Files.createDirectories(stateFile.toPath().getParent());
            objectMapper.writeValue(tempFile, state);
            try {
                Files.move(tempFile.toPath(), stateFile.toPath(), StandardCopyOption.ATOMIC_MOVE,
                        StandardCopyOption.REPLACE_EXISTING);
            } catch (java.nio.file.AtomicMoveNotSupportedException e) {
                Files.move(tempFile.toPath(), stateFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
            }
        }

        /**
         * @return persisted state, or null when no transition is pending
         * @throws IOException if the state cannot be read or decoded
         */
        public @Nullable RoleTransitionState load() throws IOException {
            if (!stateFile.exists()) {
                return null;
            }
            return objectMapper.readValue(stateFile, RoleTransitionState.class);
        }

        /**
         * Removes durable transition state after convergence.
         *
         * @throws IOException if a state file cannot be removed
         */
        public void clear() throws IOException {
            Files.deleteIfExists(tempFile.toPath());
            Files.deleteIfExists(stateFile.toPath());
        }
    }
}
