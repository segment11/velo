package io.velo.repl.content;

import io.velo.repl.ReplContent;

/**
 * Represents a Pong response in a slave-master-replication system.
 * It corresponds to the {@link io.velo.repl.ReplType#pong} message type, which is sent by a master to a slave.
 */
public class Pong extends Ping implements ReplContent {

    /**
     * Constructs a new Pong instance with the specified network listen addresses.
     *
     * @param netListenAddresses a string representing the network listen addresses
     *                           used for communication between slave and master nodes
     */
    public Pong(String netListenAddresses) {
        super(netListenAddresses);
    }
}