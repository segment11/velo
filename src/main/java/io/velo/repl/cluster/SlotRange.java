package io.velo.repl.cluster;

/**
 * Represents a range of slots.
 */
public class SlotRange implements Comparable<SlotRange> {
    /**
     * Returns the begin (inclusive) of this slot range.
     *
     * @return the begin
     */
    // for json
    public int getBegin() {
        return begin;
    }

    /**
     * Sets the begin (inclusive) of this slot range.
     *
     * @param begin the begin
     */
    public void setBegin(int begin) {
        this.begin = begin;
    }

    /**
     * Returns the end (inclusive) of this slot range.
     *
     * @return the end
     */
    public int getEnd() {
        return end;
    }

    /**
     * Sets the end (inclusive) of this slot range.
     *
     * @param end the end
     */
    public void setEnd(int end) {
        this.end = end;
    }

    int begin;

    int end;

    /**
     * Constructs an empty slot range (for JSON deserialization).
     */
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
