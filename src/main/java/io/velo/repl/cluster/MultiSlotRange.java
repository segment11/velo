package io.velo.repl.cluster;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;

/**
 * Represents multiple slot ranges.
 */
public class MultiSlotRange implements Comparable<MultiSlotRange> {
    /**
     * Returns the list of slot ranges (already sorted).
     *
     * @return the list of slot ranges
     */
    public ArrayList<SlotRange> getList() {
        return list;
    }

    /**
     * Sets the list of slot ranges.
     *
     * @param list the list of slot ranges
     */
    public void setList(ArrayList<SlotRange> list) {
        this.list = list;
    }

    // already sorted
    private ArrayList<SlotRange> list = new ArrayList<>();

    /**
     * Returns the total number of slots across all ranges.
     *
     * @return the total slot count
     */
    public int slotCount() {
        int total = 0;
        for (var slotRange : list) {
            total += slotRange.slotCount();
        }
        return total;
    }

    /**
     * Checks whether the given client slot is contained in any of the ranges.
     *
     * @param toClientSlot the client slot
     * @return true if the slot is contained
     */
    public boolean contains(int toClientSlot) {
        for (var slotRange : list) {
            if (slotRange.contains(toClientSlot)) {
                return true;
            }
        }
        return false;
    }

    @Override
    public int compareTo(@NotNull MultiSlotRange o) {
        if (list.isEmpty()) {
            return -1;
        }

        if (o.list.isEmpty()) {
            return 1;
        }

        return list.getFirst().begin - o.list.getFirst().begin;
    }

    @Override
    public String toString() {
        if (list.isEmpty()) {
            return "";
        }

        var sb = new StringBuilder();
        for (var slotRange : list) {
            sb.append(slotRange).append(",");
        }
        return sb.substring(0, sb.length() - 1);
    }

    /**
     * Adds a single slot range and keeps the list sorted.
     *
     * @param begin the inclusive begin
     * @param end   the inclusive end
     */
    public void addSingle(int begin, int end) {
        list.add(new SlotRange(begin, end));
        Collections.sort(list);
    }

    /**
     * Parses a MultiSlotRange from its string representation produced by {@link #toString()}.
     *
     * @param toString the string representation
     * @return the parsed MultiSlotRange
     * @throws IllegalArgumentException if the string is invalid
     */
    public static MultiSlotRange fromSelfString(String toString) {
        var r = new MultiSlotRange();
        r.list = new ArrayList<>();
        if (toString.isEmpty()) {
            return r;
        }

        var fromTos = toString.split(",");
        for (var ft : fromTos) {
            var fromToPair = ft.split("-");
            if (fromToPair.length != 2) {
                throw new IllegalArgumentException("Invalid from to=" + ft);
            }

            var slotRange = new SlotRange(Integer.parseInt(fromToPair[0]), Integer.parseInt(fromToPair[1]));
            if (slotRange.begin > slotRange.end) {
                throw new IllegalArgumentException("Invalid from to=" + ft);
            }

            r.list.add(slotRange);
        }

        return r;
    }

    /**
     * Converts all slot ranges into a sorted set of individual slot numbers.
     *
     * @return the sorted set of slot numbers
     */
    public TreeSet<Integer> toTreeSet() {
        TreeSet<Integer> r = new TreeSet<>();
        if (list.isEmpty()) {
            return r;
        }

        for (var one : list) {
            for (var i = one.begin; i <= one.end; i++) {
                r.add(i);
            }
        }
        return r;
    }

    private static MultiSlotRange fromSet(TreeSet<Integer> all) {
        var r = new MultiSlotRange();
        r.list = new ArrayList<>();
        if (all.isEmpty()) {
            return r;
        }

        if (all.size() == all.last() - all.first() + 1) {
            r.addSingle(all.first(), all.last());
            return r;
        }

        int begin = -1;
        int end = -1;
        int last = -1;

        for (var j : all) {
            if (begin == -1) {
                begin = j;
            }
            if (end == -1) {
                end = j;
            }
            if (last == -1) {
                last = j;
            }

            if (j != begin) {
                if (j != last + 1) {
                    r.addSingle(begin, end);
                    begin = j;
                    end = j;
                    last = j;
                    continue;
                }
            }

            last = j;
            end = j;
        }

        r.addSingle(begin, end);
        return r;
    }

    /**
     * Removes the given slots and adds the given slots, rebuilding the range list.
     *
     * @param remove the slots to remove
     * @param add    the slots to add
     */
    public void removeOrAddSet(TreeSet<Integer> remove, TreeSet<Integer> add) {
        if (remove.isEmpty() && add.isEmpty()) {
            return;
        }

        var set = toTreeSet();
        if (!remove.isEmpty()) {
            set.removeAll(remove);
        }
        if (!add.isEmpty()) {
            set.addAll(add);
        }

        var r = fromSet(set);
        list.clear();
        list.addAll(r.list);
    }

    /**
     * Adds a single client slot if it is not already present.
     *
     * @param toClientSlot the client slot to add
     */
    public void addOneSlot(int toClientSlot) {
        if (contains(toClientSlot)) {
            return;
        }

        var set = toTreeSet();
        set.add(toClientSlot);
        var r = fromSet(set);
        list.clear();
        list.addAll(r.list);
    }

    /**
     * Removes a single client slot if it is present.
     *
     * @param toClientSlot the client slot to remove
     */
    public void removeOneSlot(int toClientSlot) {
        if (!contains(toClientSlot)) {
            return;
        }

        var set = toTreeSet();
        set.remove(toClientSlot);
        var r = fromSet(set);
        list.clear();
        list.addAll(r.list);
    }

    /**
     * Returns the CLUSTER NODES formatted line(s) for the slot ranges owned by the given node.
     *
     * @param nodeId the node id
     * @param ip     the node ip
     * @param port   the node port
     * @return the formatted node line(s) including slot ranges
     */
    public ArrayList<String> clusterNodesSlotRangeList(String nodeId, String ip, Integer port) {
        ArrayList<String> list = new ArrayList<>();
        if (this.list.isEmpty()) {
            list.add(nodeId + " " + ip + " " + port + " master -");
        } else {
            var allSlotRange = this.list.stream().map(SlotRange::toString).reduce((a, b) -> a + " " + b).orElse("");
            list.add(nodeId + " " + ip + " " + port + " master - " + allSlotRange);
        }
        return list;
    }
}
