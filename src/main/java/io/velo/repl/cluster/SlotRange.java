package io.velo.repl.cluster;

public class SlotRange implements Comparable<SlotRange> {
    // for json
    public int getBegin() {
        return begin;
    }

    public void setBegin(int begin) {
        this.begin = begin;
    }

    public int getEnd() {
        return end;
    }

    public void setEnd(int end) {
        this.end = end;
    }

    int begin;

    int end;

    // for json
    public SlotRange() {
    }

    // end include
    SlotRange(int begin, int end) {
        this.begin = begin;
        this.end = end;
    }

    boolean contains(int toClientSlot) {
        return begin <= toClientSlot && toClientSlot <= end;
    }

    int slotCount() {
        return end - begin + 1;
    }

    @Override
    public String toString() {
        if (begin == end) {
            return String.valueOf(begin);
        }
        return begin + "-" + end;
    }

    @Override
    public int compareTo(SlotRange o) {
        return this.begin - o.begin;
    }
}
