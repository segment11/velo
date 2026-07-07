package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.persist.Wal;
import io.velo.repl.ReplContent;

/**
 * Represents a Ping command or message in a slave-master-replication system.
 * It corresponds to the {@link io.velo.repl.ReplType#ping} message type, which is sent by a slave to a master.
 * <p>
 * The optional cluster epoch lets a slave advertise the topology version it currently believes in, so the
 * master can compare it against its own epoch when telling the slave whether it still serves a given slot
 * (see {@link Pong}). When constructed without an epoch the wire form stays the legacy address-only bytes,
 * which the {@code bye} message relies on.
 */
public class Ping implements ReplContent {
    private final String netListenAddress;

    /**
     * The cluster epoch the sender believes in, or {@code null} for the legacy address-only encoding.
     */
    private final Integer clusterMyEpoch;

    /**
     * Field separator between the address and the trailing epoch / serve fields.
     */
    public static final String FIELD_SEPARATOR = "\n";

    /**
     * Constructs a legacy Ping carrying only the network listen address (no cluster epoch).
     *
     * @param netListenAddress a string representing the network listen addresses
     *                          used for communication between slave and master nodes
     */
    public Ping(String netListenAddress) {
        this.netListenAddress = netListenAddress;
        this.clusterMyEpoch = null;
    }

    /**
     * Constructs a Ping carrying the network listen address and the sender's cluster epoch.
     *
     * @param netListenAddress a string representing the network listen addresses
     * @param clusterMyEpoch   the cluster epoch the sender currently believes in
     */
    public Ping(String netListenAddress, int clusterMyEpoch) {
        this.netListenAddress = netListenAddress;
        this.clusterMyEpoch = clusterMyEpoch;
    }

    /**
     * Builds the string payload written on the wire. Subclasses append their own trailing fields.
     *
     * @return the encoded payload string
     */
    protected String encodePayload() {
        return clusterMyEpoch == null ? netListenAddress : netListenAddress + FIELD_SEPARATOR + clusterMyEpoch;
    }

    /**
     * Encodes the payload into the provided ByteBuf.
     *
     * @param toBuf the ByteBuf to write the payload to
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.write(Wal.keyBytes(encodePayload()));
    }

    /**
     * Returns the length of the encoded payload.
     *
     * @return the length of the encoded payload
     */
    @Override
    public int encodeLength() {
        return Wal.keyBytes(encodePayload()).length;
    }
}
