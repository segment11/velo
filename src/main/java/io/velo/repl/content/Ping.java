package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.repl.ReplContent;

/**
 * Represents a Ping command or message in a slave-master-replication system.
 * It corresponds to the {@link io.velo.repl.ReplType#ping} message type, which is sent by a slave to a master.
 */
public class Ping implements ReplContent {
    private final String netListenAddresses;

    /**
     * Constructs a new Ping instance with the specified network listen addresses.
     *
     * @param netListenAddresses a string representing the network listen addresses
     *                           used for communication between slave and master nodes
     */
    public Ping(String netListenAddresses) {
        this.netListenAddresses = netListenAddresses;
    }

    /**
     * Encodes the network listen addresses into the provided ByteBuf.
     * This method writes the network listen addresses to the byte buffer
     * to be sent from the slave to the master node.
     *
     * @param toBuf the ByteBuf to write the network listen addresses to
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.write(netListenAddresses.getBytes());
    }

    /**
     * Returns the length of the encoded network listen addresses.
     * This method provides the length of the network listen addresses,
     * which is useful for encoding and decoding purposes.
     *
     * @return the length of the encoded network listen addresses
     */
    @Override
    public int encodeLength() {
        return netListenAddresses.length();
    }
}