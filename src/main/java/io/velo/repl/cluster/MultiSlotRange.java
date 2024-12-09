package io.velo.repl.cluster;

import org.jetbrains.annotations.NotNull;

import java.util.ArrayList;
import java.util.Collections;
import java.util.TreeSet;

public class MultiSlotRange implements Comparable<MultiSlotRange> {
    public ArrayList<SlotRange> getList() {
        return list;
    }

    public void setList(ArrayList<SlotRange> list) {
        this.list = list;
    }

    // already sorted
    private ArrayList<SlotRange> list = new ArrayList<>();

    public int slotCount() {
        int total = 0;
        for (var slotRange : list) {
            total += slotRange.slotCount();
        }
        return total;
    }

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

    public void addSingle(int begin, int end) {
        list.add(new SlotRange(begin, end));
        Collections.sort(list);
    }

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
