package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

public class Ping implements ReplContent {
    private final String netListenAddresses;

    public Ping(String netListenAddresses) {
        this.netListenAddresses = netListenAddresses;
    }

    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.write(netListenAddresses.getBytes());
    }

    @Override
    public int encodeLength() {
        return netListenAddresses.length();
    }
}
