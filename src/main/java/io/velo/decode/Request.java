package io.velo.decode;

import io.velo.BaseCommand;
import io.velo.RequestHandler;
import io.velo.acl.U;
import io.velo.repl.ReplRequest;
import org.jetbrains.annotations.TestOnly;
import org.jetbrains.annotations.VisibleForTesting;

import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;

/**
 * A request containing command data, HTTP status, REPL status, and other details.
 */
public class Request {
    /** The command data as an array of byte arrays. */
    private final byte[][] data;

    /** True if this is an HTTP request. */
    private final boolean isHttp;

    /** True if this is a REPL (replication) request. */
    private final boolean isRepl;

    private ReplRequest replRequest;

    public ReplRequest getReplRequest() {
        return replRequest;
    }

    public void setReplRequest(ReplRequest replRequest) {
        this.replRequest = replRequest;
    }

    /** The command data as an array of byte arrays. */
    public byte[][] getData() {
        return data;
    }

    /** True if this is an HTTP request. */
    public boolean isHttp() {
        return isHttp;
    }

    /** True if this is a REPL request. */
    public boolean isRepl() {
        return isRepl;
    }

    /**
     * @param data   the command data as an array of byte arrays
     * @param isHttp true if this is an HTTP request
     * @param isRepl true if this is a REPL request
     */
    public Request(byte[][] data, boolean isHttp, boolean isRepl) {
        this.data = data;
        this.isHttp = isHttp;
        this.isRepl = isRepl;
    }

    /**
     * If set value is too big, do not copy, reuse read buffer.
     */
    BigStringNoMemoryCopy bigStringNoMemoryCopy;

    /** The big string no memory copy mark. */
    public BigStringNoMemoryCopy getBigStringNoMemoryCopy() {
        return bigStringNoMemoryCopy;
    }

    /** The ACL utility object for the request. */
    private U u;

    /** @return the ACL utility object */
    public U getU() {
        return u;
    }

    /**
     * @param u the ACL utility object to set
     */
    public void setU(U u) {
        this.u = u;
    }

    /**
     * @return the ACL check result
     */
    public U.CheckCmdAndKeyResult isAclCheckOk() {
        if (u == null) {
            return U.CheckCmdAndKeyResult.FALSE_WHEN_CHECK_CMD;
        }

        if (RequestHandler.AUTH_COMMAND.equals(cmd())) {
            return U.CheckCmdAndKeyResult.TRUE;
        }

        if (!u.isOn()) {
            return U.CheckCmdAndKeyResult.FALSE_WHEN_CHECK_CMD;
        }
        return u.checkCmdAndKey(cmd(), data, slotWithKeyHashList);
    }

    /** The command of the request. */
    private String cmd;

    /** The slot number for the request. */
    private short slotNumber;

    /** @return the slot number */
    public short getSlotNumber() {
        return slotNumber;
    }

    /**
     * @param slotNumber the slot number to set
     */
    public void setSlotNumber(short slotNumber) {
        this.slotNumber = slotNumber;
    }

    /** True if this is a cross-request worker. */
    private boolean isCrossRequestWorker;

    /** @return true if this is a cross-request worker */
    public boolean isCrossRequestWorker() {
        return isCrossRequestWorker;
    }

    /**
     * @param crossRequestWorker true if this is a cross-request worker
     */
    public void setCrossRequestWorker(boolean crossRequestWorker) {
        isCrossRequestWorker = crossRequestWorker;
    }

    /** The HTTP headers for the request. */
    private Map<String, String> httpHeaders;

    /**
     * @param header the header name
     * @return the header value, or null if not found
     */
    public String getHttpHeader(String header) {
        if (httpHeaders == null) {
            return null;
        }

        var headerValue = httpHeaders.get(header);
        if (headerValue != null) {
            return headerValue;
        }

        for (var entry : httpHeaders.entrySet()) {
            if (entry.getKey().equalsIgnoreCase(header)) {
                return entry.getValue();
            }
        }
        return null;
    }

    /**
     * @param httpHeaders the HTTP headers as a map
     */
    public void setHttpHeaders(Map<String, String> httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    /**
     * @param header the header name to remove
     */
    @TestOnly
    public void removeHttpHeader(String header) {
        if (httpHeaders != null) {
            httpHeaders.remove(header);
        }
    }

    /** The list of slots with key hashes associated with the request. */
    private ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList;

    /** @return the list of slots with key hashes */
    public ArrayList<BaseCommand.SlotWithKeyHash> getSlotWithKeyHashList() {
        return slotWithKeyHashList;
    }

    /**
     * @param slotWithKeyHashList the list of slots with key hashes
     */
    public void setSlotWithKeyHashList(ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList) {
        this.slotWithKeyHashList = slotWithKeyHashList;
    }

    /** Indicates that a slot can be handled by any worker. */
    public static final byte SLOT_CAN_HANDLE_BY_ANY_WORKER = -1;

    /**
     * @return the single slot number, or -1 if not applicable
     */
    public short getSingleSlot() {
        if (isRepl) {
            return replRequest.getSlot();
        }

        if (slotWithKeyHashList == null || slotWithKeyHashList.isEmpty()) {
            return SLOT_CAN_HANDLE_BY_ANY_WORKER;
        }

        var first = slotWithKeyHashList.getFirst();
        return first.slot();
    }

    @VisibleForTesting
    public static final String REPL_AS_CMD = "x-repl";

    /**
     * @return the command string
     */
    public String cmd() {
        if (isRepl) {
            return REPL_AS_CMD;
        }

        if (cmd != null) {
            return cmd;
        }
        cmd = new String(data[0]).toLowerCase(Locale.ROOT);
        return cmd;
    }

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
