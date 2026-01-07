package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.repl.ReplContent;

/**
 * Represents a content type for sending a greeting or initialization message from a slave to a master.
 * This message includes the slave's unique identifier and the network addresses it listens on.
 * It corresponds to the {@link io.velo.repl.ReplType#hello} message type, which is sent by a slave to a master.
 */
public class Hello implements ReplContent {
    /**
     * The unique identifier for the slave.
     */
    private final long slaveUuid;

    /**
     * A string containing network addresses the slave listens on.
     */
    private final String netListenAddresses;

    /**
     * Constructs a new {@code Hello} message with the specified slave UUID and network listen addresses.
     *
     * @param slaveUuid          the unique identifier for the slave
     * @param netListenAddresses a string containing network addresses the slave listens on
     */
    public Hello(long slaveUuid, String netListenAddresses) {
        this.slaveUuid = slaveUuid;
        this.netListenAddresses = netListenAddresses;
    }

    /**
     * Encodes the content of this message into the provided {@link ByteBuf}.
     * The encoding format consists of:
     * - 8 bytes for the slave UUID (written as a long)
     * - 4 bytes for net listen addresses length
     * - 2 bytes for slot number
     * - 4 + 4 + 1 + 4 + 4 for ReplProperties
     * - The byte array representation of {@code netListenAddresses}
     *
     * @param toBuf the buffer to which the message content will be written
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        toBuf.writeLong(slaveUuid);
        toBuf.writeInt(netListenAddresses.length());
        toBuf.write(netListenAddresses.getBytes());

        writeReplProperties(toBuf);
    }

    static void writeReplProperties(ByteBuf toBuf) {
        toBuf.writeShort(ConfForGlobal.slotNumber);
        var replProperties = ConfForSlot.global.generateReplProperties();
        toBuf.writeInt(replProperties.bucketsPerSlot());
        toBuf.writeInt(replProperties.segmentNumberPerFd());
        toBuf.writeByte(replProperties.fdPerChunk());
        toBuf.writeInt(replProperties.segmentLength());
        toBuf.writeInt(replProperties.oneChargeBucketNumber());
    }

    /**
     * Calculates and returns the total length in bytes required to encode this message.
     * The length is computed as the sum of:
     * - 8 bytes for the slave UUID
     * - The length of the byte array representation of {@code netListenAddresses}
     *
     * @return the length in bytes required to encode this message
     */
    @Override
    public int encodeLength() {
        return 8 + 4 + netListenAddresses.length() + 2 + 17;
    }
}