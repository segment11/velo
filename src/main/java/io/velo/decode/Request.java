package io.velo.decode;

import io.velo.BaseCommand;
import io.velo.RequestHandler;
import io.velo.acl.U;
import org.jetbrains.annotations.TestOnly;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Map;

/**
 * Represents a request in the Velo framework, containing command data, HTTP status, REPL status,
 * and other relevant request details.
 */
public class Request {
    /**
     * Stores the command data as an array of byte arrays.
     */
    private final byte[][] data;

    /**
     * Indicates whether the request is an HTTP request.
     */
    private final boolean isHttp;

    /**
     * Indicates whether the request is a REPL (Read-Eval-Print Loop) request.
     */
    private final boolean isRepl;

    /**
     * Retrieves the command data as an array of byte arrays.
     *
     * @return the command data
     */
    public byte[][] getData() {
        return data;
    }

    /**
     * Checks if the request is an HTTP request.
     *
     * @return true if the request is an HTTP request, false otherwise
     */
    public boolean isHttp() {
        return isHttp;
    }

    /**
     * Checks if the request is a REPL request.
     *
     * @return true if the request is a REPL request, false otherwise
     */
    public boolean isRepl() {
        return isRepl;
    }

    /**
     * Constructs a new Request object with the specified command data and request types.
     *
     * @param data   the command data as an array of byte arrays
     * @param isHttp true if the request is an HTTP request, false otherwise
     * @param isRepl true if the request is a REPL request, false otherwise
     */
    public Request(byte[][] data, boolean isHttp, boolean isRepl) {
        this.data = data;
        this.isHttp = isHttp;
        this.isRepl = isRepl;
    }

    /**
     * The ACL (Access Control List) utility object for the request.
     */
    private U u;

    /**
     * Retrieves the ACL utility object for the request.
     *
     * @return the ACL utility object
     */
    public U getU() {
        return u;
    }

    /**
     * Sets the ACL utility object for the request.
     *
     * @param u the ACL utility object to set
     */
    public void setU(U u) {
        this.u = u;
    }

    /**
     * Checks if the ACL (Access Control List) check is okay for the request.
     *
     * @return the result of the ACL check
     */
    public U.CheckCmdAndKeyResult isAclCheckOk() {
        // just when do unit test
        if (u == null) {
            return U.CheckCmdAndKeyResult.TRUE;
        }

        if (RequestHandler.AUTH_COMMAND.equals(cmd())) {
            return U.CheckCmdAndKeyResult.TRUE;
        }

        if (!u.isOn()) {
            return U.CheckCmdAndKeyResult.FALSE_WHEN_CHECK_CMD;
        }
        return u.checkCmdAndKey(cmd(), data, slotWithKeyHashList);
    }

    /**
     * The command of the request.
     */
    private String cmd;

    /**
     * The slot number for the request.
     */
    private short slotNumber;

    /**
     * Retrieves the slot number for the request.
     *
     * @return the slot number
     */
    public short getSlotNumber() {
        return slotNumber;
    }

    /**
     * Sets the slot number for the request.
     *
     * @param slotNumber the slot number to set
     */
    public void setSlotNumber(short slotNumber) {
        this.slotNumber = slotNumber;
    }

    /**
     * Indicates if the request is a cross-request worker.
     */
    private boolean isCrossRequestWorker;

    /**
     * Checks if the request is a cross-request worker.
     *
     * @return true if the request is a cross-request worker, false otherwise
     */
    public boolean isCrossRequestWorker() {
        return isCrossRequestWorker;
    }

    /**
     * Sets if the request is a cross-request worker.
     *
     * @param crossRequestWorker true if the request is a cross-request worker, false otherwise
     */
    public void setCrossRequestWorker(boolean crossRequestWorker) {
        isCrossRequestWorker = crossRequestWorker;
    }

    /**
     * The HTTP headers for the request.
     */
    private Map<String, String> httpHeaders;

    /**
     * Retrieves the value of a specific HTTP header.
     *
     * @param header the header name
     * @return the header value, or null if the header does not exist
     */
    public String getHttpHeader(String header) {
        return httpHeaders == null ? null : httpHeaders.get(header);
    }

    /**
     * Sets the HTTP headers for the request.
     *
     * @param httpHeaders the HTTP headers as a map
     */
    public void setHttpHeaders(Map<String, String> httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    /**
     * Removes a specific HTTP header from the request.
     *
     * @param header the header name to remove
     */
    @TestOnly
    public void removeHttpHeader(String header) {
        if (httpHeaders != null) {
            httpHeaders.remove(header);
        }
    }

    /**
     * The list of slots with key hashes associated with the request.
     */
    private ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList;

    /**
     * Retrieves the list of slots with key hashes associated with the request.
     *
     * @return the list of slots with key hashes
     */
    public ArrayList<BaseCommand.SlotWithKeyHash> getSlotWithKeyHashList() {
        return slotWithKeyHashList;
    }

    /**
     * Sets the list of slots with key hashes for the request.
     *
     * @param slotWithKeyHashList the list of slots with key hashes
     */
    public void setSlotWithKeyHashList(ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList) {
        this.slotWithKeyHashList = slotWithKeyHashList;
    }

    /**
     * Indicates that a slot can be handled by any worker.
     */
    public static final byte SLOT_CAN_HANDLE_BY_ANY_WORKER = -1;

    /**
     * Retrieves the single slot number associated with the request.
     *
     * @return the single slot number, or -1 if not applicable
     */
    public short getSingleSlot() {
        if (isRepl) {
            // refer to Repl.decode, byte[1] == two length bytes
            return ByteBuffer.wrap(data[1]).getShort();
        }

        if (slotWithKeyHashList == null || slotWithKeyHashList.isEmpty()) {
            return SLOT_CAN_HANDLE_BY_ANY_WORKER;
        }

        var first = slotWithKeyHashList.getFirst();
        return first.slot();
    }

    /**
     * Retrieves the command of the request as a string.
     *
     * @return the command string
     */
    public String cmd() {
        if (cmd != null) {
            return cmd;
        }
        cmd = new String(data[0]).toLowerCase();
        return cmd;
    }

    /**
     * Provides a string representation of the request, including the command and request properties.
     *
     * @return the request as a string
     */
    @Override
    public String toString() {
        return "Request{" +
                "cmd=" + cmd() +
                ", data.length=" + data.length +
                ", isHttp=" + isHttp +
                ", isRepl=" + isRepl +
                '}';
    }
}