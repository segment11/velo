package io.velo.repl.content;

import io.activej.bytebuf.ByteBuf;
import io.velo.ConfForGlobal;
import io.velo.ConfForSlot;
import io.velo.persist.Wal;
import io.velo.repl.ReplContent;

/**
 * Greeting message from slave to master, includes slave UUID and network addresses.
 */
public class Hello implements ReplContent {
    private final long slaveUuid;
    private final String netListenAddress;

    /**
     * Constructs a Hello message.
     *
     * @param slaveUuid         the slave's UUID
     * @param netListenAddress  the slave's network listen address
     */
    public Hello(long slaveUuid, String netListenAddress) {
        this.slaveUuid = slaveUuid;
        this.netListenAddress = netListenAddress;
    }

    /**
     * Encodes the content of this message into the provided {@link ByteBuf}.
     * Encoding format:
     * - 8 bytes for slave UUID
     * - 4 bytes for net listen addresses length
     * - 2 bytes for slot number
     * - 4 + 4 + 4 + 1 + 4 + 1 for ReplProperties
     * - netListenAddress bytes
     *
     * @param toBuf the buffer to which the message content will be written
     */
    @Override
    public void encodeTo(ByteBuf toBuf) {
        var addressBytes = Wal.keyBytes(netListenAddress);
        toBuf.writeLong(slaveUuid);
        toBuf.writeInt(addressBytes.length);
        toBuf.write(addressBytes);

        writeReplProperties(toBuf);
    }

    static void writeReplProperties(ByteBuf toBuf) {
        toBuf.writeShort(ConfForGlobal.slotNumber);
        var replProperties = ConfForSlot.global.generateReplProperties();
        toBuf.writeInt(replProperties.bucketsPerSlot());
        toBuf.writeInt(replProperties.oneChargeBucketNumber());
        toBuf.writeInt(replProperties.segmentNumberPerFd());
        toBuf.writeByte(replProperties.fdPerChunk());
        toBuf.writeInt(replProperties.segmentLength());
        toBuf.writeByte(replProperties.isSegmentUseCompression() ? (byte) 1 : (byte) 0);
    }

    /**
     * Calculates and returns the total length in bytes required to encode this message.
     * Length breakdown: 8 (UUID) + 4 (length) + netListenAddress.length() + 2 (slot) + 18 (ReplProperties)
     *
     * @return the length in bytes required to encode this message
     */
    @Override
    public int encodeLength() {
        return 8 + 4 + Wal.keyBytes(netListenAddress).length + 2 + 18;
    }
}
