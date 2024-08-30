package io.velo.repl.content;

import io.velo.repl.ReplContent;

public class Pong extends Ping implements ReplContent {
    public Pong(String netListenAddresses) {
        super(netListenAddresses);
    }
}
