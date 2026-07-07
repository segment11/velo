package io.velo.repl.content;

import io.velo.repl.ReplContent;

/**
 * Represents a Pong response in a slave-master-replication system.
 * It corresponds to the {@link io.velo.repl.ReplType#pong} message type, which is sent by a master to a slave.
 * <p>
 * Beyond the master's listen address and cluster epoch, a Pong can carry a {@code serveSlot} flag telling the
 * slave whether this master still serves the slot the ping was for. A slave that receives {@code serveSlot=false}
 * from a master whose epoch is at least as new as its own drops its slave repl pair for that slot, so it stops
 * replicating a slot the master's shard no longer owns (e.g. an always-initialized fallback slot, or a slot the
 * master is still importing during migration). When constructed without the serve flag the wire form stays the
 * legacy encoding used by {@code byeBye}.
 */
public class Pong extends Ping implements ReplContent {

    /**
     * Whether this master serves the pinged slot for the slave, or {@code null} for the legacy encoding.
     */
    private final Boolean serveSlot;

    /**
     * Constructs a legacy Pong carrying only the network listen address.
     *
     * @param netListenAddress a string representing the network listen addresses
     *                          used for communication between slave and master nodes
     */
    public Pong(String netListenAddress) {
        super(netListenAddress);
        this.serveSlot = null;
    }

    /**
     * Constructs a Pong carrying the master's listen address, cluster epoch, and whether it serves the slot.
     *
     * @param netListenAddress the master's network listen addresses
     * @param clusterMyEpoch   the master's current cluster epoch
     * @param serveSlot        true if this master still serves the pinged slot for the slave
     */
    public Pong(String netListenAddress, int clusterMyEpoch, boolean serveSlot) {
        super(netListenAddress, clusterMyEpoch);
        this.serveSlot = serveSlot;
    }

    @Override
    protected String encodePayload() {
        var base = super.encodePayload();
        return serveSlot == null ? base : base + FIELD_SEPARATOR + (serveSlot ? "1" : "0");
    }
}
