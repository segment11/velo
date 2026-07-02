package io.velo.command;

import java.util.Set;

/**
 * Immutable metadata for one Redis-compatible command, consumed by the
 * {@code COMMAND} family of subcommands ({@code COMMAND INFO}, {@code COMMAND DOCS},
 * {@code COMMAND LIST}, {@code COMMAND COUNT}).
 *
 * <p>Mirrors only the fields of Redis {@code struct redisCommand} that the
 * {@code COMMAND} replies actually emit. See
 * {@code doc/design/19_command_registry_design.md}.
 *
 * @param name           lowercase command name (e.g. {@code "get"})
 * @param arity          argument count including the command name;
 *                       {@code > 0} means exact, {@code < 0} means at least {@code |arity|}
 * @param flags          command flags such as {@code "write"}, {@code "readonly"},
 *                       {@code "fast"}, {@code "movablekeys"} (lowercase)
 * @param firstKey       index of the first key argument, or {@code -1} if the command
 *                       takes no keys / has movable keys
 * @param lastKey        index of the last key argument, or {@code -1} meaning "to end"
 * @param keyStep        distance between consecutive keys; {@code 0} for movable-key commands
 * @param aclCategories  ACL categories with leading {@code @}, e.g. {@code "@read"}, {@code "@string"}
 * @param group          functional group, e.g. {@code "string"}, {@code "hash"}, {@code "server"}
 * @param summary        short one-line description
 * @param since          the version that added the command
 * @param complexity     Big-O complexity string, or {@code null} to omit from {@code COMMAND DOCS}
 */
public record CommandEntry(
        String name,
        int arity,
        Set<String> flags,
        int firstKey,
        int lastKey,
        int keyStep,
        Set<String> aclCategories,
        String group,
        String summary,
        String since,
        String complexity
) {
}
