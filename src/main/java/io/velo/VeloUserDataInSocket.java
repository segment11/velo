package io.velo;

import io.velo.repl.ReplPair;

/**
 * A class to store user-specific data for a socket connection in the Velo system.
 * This class holds various metadata and configuration settings related to a client connection.
 */
public class VeloUserDataInSocket {
    /**
     * The time when the client was established.
     */
    long connectedTimeMillis = System.currentTimeMillis();

    /**
     * The time when the last command was sent.
     */
    long lastSendCommandTimeMillis = System.currentTimeMillis();

    /**
     * The last command sent by the client.
     */
    String lastSendCommand = null;

    /**
     * The number of commands sent by the client.
     */
    long sendCommandCount = 0;

    /**
     * The number of bytes sent from the client.
     */
    long netInBytesLength = 0;
    /**
     * The number of bytes sent to the client.
     */
    long netOutBytesLength = 0;

    /**
     * The authenticated user associated with the socket connection.
     */
    String authUser = null;

    /**
     * Indicates whether the connection uses RESP3 protocol.
     */
    boolean isResp3 = false;

    /**
     * The name of the library used by the client.
     */
    String libName = null;

    /**
     * Sets the name of the library used by the client.
     *
     * @param libName The name of the library used by the client.
     */
    public void setLibName(String libName) {
        this.libName = libName;
    }

    /**
     * The version of the library used by the client.
     */
    String libVer = null;

    /**
     * Sets the version of the library used by the client.
     *
     * @param libVer The version of the library used by the client.
     */
    public void setLibVer(String libVer) {
        this.libVer = libVer;
    }

    /**
     * Indicates whether the connection is read-only.
     */
    boolean isConnectionReadonly = false;

    /**
     * Enum to represent the reply mode for the socket connection.
     */
    public enum ReplyMode {
        on, off, skip;

        /**
         * Converts a string to the corresponding ReplyMode enum value.
         *
         * @param str The string representation of the reply mode.
         * @return The ReplyMode enum value.
         */
        public static ReplyMode from(String str) {
            return switch (str) {
                case "off" -> off;
                case "skip" -> skip;
                default -> on;
            };
        }
    }

    /**
     * The current reply mode for the socket connection.
     */
    ReplyMode replyMode = ReplyMode.on;

    /**
     * Sets the reply mode for the socket connection.
     *
     * @param replyMode The new reply mode.
     */
    public void setReplyMode(ReplyMode replyMode) {
        this.replyMode = replyMode;
    }

    /**
     * The last scan assign cursor used in the socket connection.
     */
    private long lastScanAssignCursor = 0;

    /**
     * Returns the last scan assign cursor.
     *
     * @return The last scan assign cursor.
     */
    public long getLastScanAssignCursor() {
        return lastScanAssignCursor;
    }

    /**
     * Sets the last scan assign cursor.
     *
     * @param lastScanAssignCursor The new last scan assign cursor.
     */
    public void setLastScanAssignCursor(long lastScanAssignCursor) {
        this.lastScanAssignCursor = lastScanAssignCursor;
    }

    /**
     * The beginning scan sequence for the socket connection.
     */
    private long beginScanSeq = 0;

    /**
     * Returns the beginning scan sequence.
     *
     * @return The beginning scan sequence.
     */
    public long getBeginScanSeq() {
        return beginScanSeq;
    }

    /**
     * Sets the beginning scan sequence.
     *
     * @param beginScanSeq The new beginning scan sequence.
     */
    public void setBeginScanSeq(long beginScanSeq) {
        this.beginScanSeq = beginScanSeq;
    }

    /**
     * The name of the client associated with the socket connection.
     */
    private String clientName;

    /**
     * Returns the name of the client.
     *
     * @return The name of the client.
     */
    public String getClientName() {
        return clientName;
    }

    /**
     * Sets the name of the client.
     *
     * @param clientName The new name of the client.
     */
    public void setClientName(String clientName) {
        this.clientName = clientName;
    }

    /**
     * The ReplPair representing the slave in the TCP client for the socket connection.
     */
    ReplPair replPairAsSlaveInTcpClient = null;

    /**
     * Constructs a new instance of VeloUserDataInSocket with default values.
     */
    public VeloUserDataInSocket() {
    }

    /**
     * Constructs a new instance of VeloUserDataInSocket with the specified ReplPair.
     *
     * @param replPairAsSlaveInTcpClient The ReplPair representing the slave in the TCP client.
     */
    public VeloUserDataInSocket(ReplPair replPairAsSlaveInTcpClient) {
        this.replPairAsSlaveInTcpClient = replPairAsSlaveInTcpClient;
    }
}