package io.velo.repl.support;

import redis.clients.jedis.commands.ProtocolCommand;

// for jedis
public class ExtendProtocolCommand implements ProtocolCommand {
    private final String command;

    public ExtendProtocolCommand(String command) {
        this.command = command;
    }

    @Override
    public byte[] getRaw() {
        return command.getBytes();
    }
}
