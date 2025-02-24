package io.velo.repl.support;

import redis.clients.jedis.commands.ProtocolCommand;

/**
 * Represents an extended Redis protocol command. This class allows you to define custom commands
 * that can be sent to a Redis server using a Jedis client.
 *
 * @since 1.0.0
 */
public class ExtendProtocolCommand implements ProtocolCommand {

    private final String command;

    /**
     * Constructs a new instance of ExtendProtocolCommand with the specified command.
     *
     * @param command The command name as a string.
     * @throws IllegalArgumentException if the provided command is null or empty.
     * @since 1.0.0
     */
    public ExtendProtocolCommand(String command) {
        if (command == null || command.isEmpty()) {
            throw new IllegalArgumentException("Command cannot be null or empty");
        }
        this.command = command;
    }

    /**
     * Returns the raw byte array representation of the command. This method is typically
     * used by Jedis to convert the command into the format expected by the Redis server.
     *
     * @return A byte array representing the command.
     * @since 1.0.0
     */
    @Override
    public byte[] getRaw() {
        return command.getBytes();
    }
}