package io.velo.decode;

import io.velo.BaseCommand;

import java.util.ArrayList;
import java.util.Map;

public class Request {
    private final byte[][] data;

    private final boolean isHttp;
    private final boolean isRepl;

    public byte[][] getData() {
        return data;
    }

    public boolean isHttp() {
        return isHttp;
    }

    public boolean isRepl() {
        return isRepl;
    }

    public Request(byte[][] data, boolean isHttp, boolean isRepl) {
        this.data = data;
        this.isHttp = isHttp;
        this.isRepl = isRepl;
    }

    private String cmd;

    private short slotNumber;

    public short getSlotNumber() {
        return slotNumber;
    }

    public void setSlotNumber(short slotNumber) {
        this.slotNumber = slotNumber;
    }

    private boolean isCrossRequestWorker;

    public boolean isCrossRequestWorker() {
        return isCrossRequestWorker;
    }

    public void setCrossRequestWorker(boolean crossRequestWorker) {
        isCrossRequestWorker = crossRequestWorker;
    }

    private Map<String, String> httpHeaders;

    public String getHttpHeader(String header) {
        return httpHeaders == null ? null : httpHeaders.get(header);
    }

    public void setHttpHeaders(Map<String, String> httpHeaders) {
        this.httpHeaders = httpHeaders;
    }

    private ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList;

    public ArrayList<BaseCommand.SlotWithKeyHash> getSlotWithKeyHashList() {
        return slotWithKeyHashList;
    }

    public void setSlotWithKeyHashList(ArrayList<BaseCommand.SlotWithKeyHash> slotWithKeyHashList) {
        this.slotWithKeyHashList = slotWithKeyHashList;
    }

    public static final byte SLOT_CAN_HANDLE_BY_ANY_WORKER = -1;

    public short getSingleSlot() {
        if (isRepl) {
            // refer to Repl.decode
            return data[1][0];
        }

        if (slotWithKeyHashList == null || slotWithKeyHashList.isEmpty()) {
            return SLOT_CAN_HANDLE_BY_ANY_WORKER;
        }

        var first = slotWithKeyHashList.getFirst();
        return first.slot();
    }

    public String cmd() {
        if (cmd != null) {
            return cmd;
        }
        cmd = new String(data[0]).toLowerCase();
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
