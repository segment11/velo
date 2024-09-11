package io.velo.tools;

import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;

public class RadixTree {
    // only support readable chars
    public static final byte BYTE_LENGTH = 50;
    public static final byte[] READABLE_BYTES = new byte[BYTE_LENGTH];
    public static final byte[] BYTE_INDEX_ARRAY = new byte[Byte.MAX_VALUE];

    static {
        Arrays.fill(BYTE_INDEX_ARRAY, (byte) -1);

        for (int i = 0; i < 10; i++) {
            READABLE_BYTES[i] = String.valueOf(i).getBytes()[0];
            BYTE_INDEX_ARRAY[READABLE_BYTES[i]] = (byte) i;
        }

        for (int i = 0; i <= 26; i++) {
            byte b = (byte) (97 + i);
            READABLE_BYTES[10 + i] = b;
            BYTE_INDEX_ARRAY[b] = (byte) (10 + i);
        }

        var extendBytes = ":+-_.|/\\()[]{}".getBytes();
        for (int i = 0; i < extendBytes.length; i++) {
            byte b = extendBytes[i];
            READABLE_BYTES[10 + 26 + i] = b;
            BYTE_INDEX_ARRAY[b] = (byte) (10 + 26 + i);
        }
    }

    // max key length
    public static final byte MAX_LENGTH = 32;

    private static final byte TYPE_INT = Byte.MIN_VALUE;

    private final Node root = new Node();

    public long sumIncr(Node current, StringBuilder sb, int[] nextLevelBeginIndex) {
        if (current.bytes != null) {
            sb.append(new String(current.bytes));
        }
        boolean isFindBegin = false;

        long sum = current.content != null ? ByteBuffer.wrap(current.content).position(1).getInt() : 0;
        int sumN = -1;
        for (int i = 0; i < current.children.length; i++) {
            var node = current.children[i];
            if (node == null) {
                continue;
            }

            sumN++;
            if ((nextLevelBeginIndex.length > 0 && sumN == nextLevelBeginIndex[0]) || nextLevelBeginIndex.length == 0) {
                if (!isFindBegin) {
                    sb.append("(");
                    isFindBegin = true;
                }
                sb.append(new String(node.bytes)).append(",");

                if (node.isWord) {
                    sum += ByteBuffer.wrap(node.content).position(1).getInt();
                } else {
                    int[] nextNextLevelBeginIndex = new int[0];
                    if (nextLevelBeginIndex.length > 0) {
                        nextNextLevelBeginIndex = new int[nextLevelBeginIndex.length - 1];
                        if (nextNextLevelBeginIndex.length > 0) {
                            System.arraycopy(nextLevelBeginIndex, 1, nextNextLevelBeginIndex, 0, nextNextLevelBeginIndex.length);
                        }
                    }
                    sum += sumIncr(node, sb, nextNextLevelBeginIndex);
                }
            }
        }
        if (isFindBegin) {
            if (sb.charAt(sb.length() - 1) == ',') {
                sb.deleteCharAt(sb.length() - 1);
            }
            sb.append(")");
        }

        return sum;
    }

    public void sumIncrFromRoot(int[] nextLevelBeginIndex) {
        var sb = new StringBuilder();
        long sum = sumIncr(root, sb, nextLevelBeginIndex);
        System.out.println(sb + "=" + sum);
    }

    private byte[] insertRec(ByteBuffer buffer, byte[] content, Node current, boolean isIncr) {
        if (buffer == null || !buffer.hasRemaining()) {
            return null;
        }

        buffer.mark();
        byte first = buffer.get();
        buffer.reset();
        byte index = BYTE_INDEX_ARRAY[first];
        if (index == -1) {
            throw new IllegalArgumentException("Not supported byte=" + first);
        }

        var node = current.children[index];
        if (node == null) {
            node = new Node();
            node.isWord = true;

            if (isIncr) {
                node.content = new byte[5];
                ByteBuffer.wrap(node.content).put(TYPE_INT).putInt(1);
            } else {
                node.content = content;
            }

            byte[] dst = new byte[buffer.remaining()];
            buffer.get(dst);
            node.bytes = dst;

            current.children[index] = node;
            return node.content;
        }

        // prefixLength >= 1
        int prefixLength = prefixLength(node.bytes, buffer);
        if (prefixLength == node.bytes.length) {
            if (prefixLength == buffer.remaining()) {
                // equals
                node.isWord = true;

                if (isIncr) {
                    if (node.content == null) {
                        node.content = new byte[5];
                    }
                    var bufferInner = ByteBuffer.wrap(node.content);
                    var old = bufferInner.position(1).getInt();
                    bufferInner.position(0).put(TYPE_INT).putInt(old + 1);
                } else {
                    node.content = content;
                }
                return node.content;
            } else {
                // start with node bytes
                buffer.position(buffer.position() + node.bytes.length);
                return insertRec(buffer, content, node, isIncr);
            }
        } else if (prefixLength == buffer.remaining()) {
            // start with buffer bytes
            var newChild = new Node();

            byte[] newChildDst = new byte[node.bytes.length - buffer.remaining()];
            ByteBuffer.wrap(node.bytes).position(buffer.remaining()).get(newChildDst);
            newChild.bytes = newChildDst;
            newChild.isWord = node.isWord;
            newChild.children = node.children;

            byte[] dst = new byte[buffer.remaining()];
            buffer.get(dst);

            // add the new node
            node.isWord = true;

            if (isIncr) {
                if (node.content == null) {
                    node.content = new byte[5];
                }
                var bufferInner = ByteBuffer.wrap(node.content);
                var old = bufferInner.position(1).getInt();
                bufferInner.position(0).put(TYPE_INT).putInt(old + 1);
            } else {
                node.content = content;
            }

            node.bytes = dst;
            node.children = new Node[BYTE_LENGTH];
            node.children[BYTE_INDEX_ARRAY[newChildDst[0]]] = newChild;

            return node.content;
        } else {
            var newChild = new Node();

            byte[] newChildDst = new byte[node.bytes.length - prefixLength];
            ByteBuffer.wrap(node.bytes).position(prefixLength).get(newChildDst);

            newChild.bytes = newChildDst;
            newChild.isWord = node.isWord;
            newChild.children = node.children;
            newChild.content = node.content;

            byte[] dst = new byte[prefixLength];
            ByteBuffer.wrap(node.bytes).get(dst);

            // add the differing part of the existing node
            node.isWord = false;
            node.content = null;
            node.bytes = dst;
            node.children = new Node[BYTE_LENGTH];
            node.children[BYTE_INDEX_ARRAY[newChildDst[0]]] = newChild;

            // insert the differing part of the inserting string recursively
            buffer.position(buffer.position() + prefixLength);
            return insertRec(buffer, content, node, isIncr);
        }
    }

    private int prefixLength(byte[] bytes, ByteBuffer buffer) {
        int n = 0;
        buffer.mark();
        try {
            for (int i = 0; i < bytes.length; i++) {
                byte b = bytes[i];
                if (buffer.hasRemaining() && b == buffer.get()) {
                    n++;
                } else {
                    break;
                }
            }
            return n;
        } finally {
            buffer.reset();
        }
    }

    // user byte buffer so need no copy
    public void insert(ByteBuffer buffer, byte[] content) {
        insertRec(buffer, content, root, false);
    }

    public int incr(ByteBuffer buffer) {
        var x = insertRec(buffer, null, root, true);
        return ByteBuffer.wrap(x).position(1).getInt();
    }

    private byte[] removeRec(ByteBuffer buffer, Node current) {
        if (buffer == null || !buffer.hasRemaining()) {
            return null;
        }

        buffer.mark();
        byte first = buffer.get();
        buffer.reset();
        byte index = BYTE_INDEX_ARRAY[first];

        var node = current.children[index];

        if (node == null) {
            return null;
        }

        int prefixLength = prefixLength(node.bytes, buffer);
        if (prefixLength == node.bytes.length && prefixLength == buffer.remaining()) {
            var childrenCount = (int) Arrays.stream(node.children).filter(Objects::nonNull).count();
            if (childrenCount == 0) {
                current.children[index] = null;
            } else if (childrenCount == 1) {
                var newChild = Arrays.stream(node.children).filter(Objects::nonNull).findFirst().get();

                var newBytes = new byte[node.bytes.length + newChild.bytes.length];
                var newBuffer = ByteBuffer.wrap(newBytes);
                newBuffer.put(node.bytes);
                newBuffer.put(newChild.bytes);

                node.bytes = newBytes;
                node.children = newChild.children;
                node.isWord = newChild.isWord;
            } else {
                node.isWord = false;
                return node.content;
            }

            var r = node.content;
            node.content = null;
            return r;
        } else if (prefixLength == node.bytes.length) {
            buffer.position(buffer.position() + prefixLength);
            return removeRec(buffer, node);
        }
        return null;
    }

    public byte[] remove(ByteBuffer buffer) {
        return removeRec(buffer, root);
    }

    public byte[] get(ByteBuffer buffer) {
        return getRec(buffer, root);
    }

    public byte[] getRec(ByteBuffer buffer, Node current) {
        if (buffer == null || !buffer.hasRemaining()) {
            return null;
        }

        buffer.mark();
        byte first = buffer.get();
        buffer.reset();
        byte index = BYTE_INDEX_ARRAY[first];

        var node = current.children[index];

        if (node == null) {
            return null;
        }

        int prefixLength = prefixLength(node.bytes, buffer);
        if (prefixLength == node.bytes.length && prefixLength == buffer.remaining()) {
            if (node.isWord) {
                return node.content;
            } else {
                return null;
            }
        }

        if (prefixLength == node.bytes.length) {
            // start with node bytes
            buffer.position(buffer.position() + prefixLength);
            return getRec(buffer, node);
        }

        return null;
    }

    public void print(PrintCallback callback) {
        printRec("", root, callback);
    }

    public void printRec(String str, Node current, PrintCallback callback) {
        if (current.isWord) {
            if (current.content.length == 5 && current.content[0] == TYPE_INT) {
                int x = ByteBuffer.wrap(current.content).position(1).getInt();
                callback.call(str, String.valueOf(x).getBytes());
            } else {
                callback.call(str, current.content);
            }
        }
        for (int i = 0; i < current.children.length; i++) {
            var node = current.children[i];
            if (node != null) {
                printRec(str + new String(node.bytes), node, callback);
            }
        }
    }

    public static void printByteIndex() {
        for (int i = 0; i < READABLE_BYTES.length; i++) {
            System.out.println(i + "=" + READABLE_BYTES[i]);
        }

        for (int i = 0; i < BYTE_INDEX_ARRAY.length; i++) {
            byte index = BYTE_INDEX_ARRAY[i];
            if (index != -1) {
                System.out.println(i + " index=" + index);
            }
        }
    }

    public void insertByStringForTest(String str) {
        insertByString(str, str.getBytes());
    }

    public void insertByStringForTest2(String str) {
        insertByString(str, (str + "xxx").getBytes());
    }

    public void insertByString(String str, byte[] content) {
        insertByBytes(str.getBytes(), content);
    }

    public void insertByBytes(byte[] bytes, byte[] content) {
        insert(ByteBuffer.wrap(bytes), content);
    }

    public int incrByString(String str) {
        return incr(ByteBuffer.wrap(str.getBytes()));
    }

    public int incrByBytes(byte[] bytes) {
        return incr(ByteBuffer.wrap(bytes));
    }

    public byte[] getByString(String str) {
        return getByBytes(str.getBytes());
    }

    public byte[] getByBytes(byte[] bytes) {
        return get(ByteBuffer.wrap(bytes));
    }

    public byte[] removeByString(String str) {
        return removeByBytes(str.getBytes());
    }

    public byte[] removeByBytes(byte[] bytes) {
        return remove(ByteBuffer.wrap(bytes));
    }

    public static void main(String[] args) {
//        printByteIndex();

        var rt = new RadixTree();
//        rt.insertByStringForTest("hello");
//        rt.insertByStringForTest("apple");
//        rt.insertByStringForTest("banana");
//        rt.insertByStringForTest("ban{ana");
//        rt.insertByStringForTest("ban}ana");
//        rt.insertByStringForTest("cat");
//        rt.insertByStringForTest("dog");
//        rt.insertByStringForTest("elephant");
//        rt.insertByStringForTest("fish");
//        rt.insertByStringForTest("grape");
//        rt.insertByStringForTest("pineapple");
//        rt.insertByStringForTest("zebra");
//        rt.insertByStringForTest("zoo");
//        rt.insertByStringForTest("graph");
//
//        // again
//        rt.insertByStringForTest2("hello");
//        rt.insertByStringForTest2("banana");
//        rt.insertByStringForTest2("cat");
//        rt.insertByStringForTest2("elephant");
//        rt.insertByStringForTest2("grape");
//        rt.insertByStringForTest2("zebra");
//        rt.insertByStringForTest2("graph");

        for (int i = 0; i < 10; i++) {
            System.out.println("incr xxx result=" + rt.incrByString("xxx"));
            System.out.println("incr xyz result=" + rt.incrByString("xyz"));
            System.out.println("incr xyab result=" + rt.incrByString("xyab"));
            System.out.println("incr yab result=" + rt.incrByString("yab"));
            System.out.println("incr ybc result=" + rt.incrByString("ybc"));
        }

//        rt.sumIncrFromRoot(new int[]{0, 1, 1});
        rt.sumIncrFromRoot(new int[]{1});

//        System.out.println("------------");
//        rt.print((str, content) -> {
//            System.out.println(str + "=" + new String(content));
//        });
//        System.out.println("------------");
//
//        System.out.println("remove cat result=" + new String(rt.removeByString("cat")));
//        System.out.println("remove dog result=" + new String(rt.removeByString("dog")));
//        byte[] dog1s = rt.removeByString("dog1");
//        System.out.println("remove dog1 result=" + (dog1s == null ? "null" : new String(dog1s)));
//
//        System.out.println("------------");
//        rt.print((str, content) -> {
//            System.out.println(str + "=" + new String(content));
//        });
//        System.out.println("------------");
//
//        var r = rt.getByString("ban}ana");
//        System.out.println("get ban}ana result=" + new String(r));
    }

    static interface PrintCallback {
        void call(String x, byte[] content);
    }

    static class Node {
        Node[] children;
        byte[] bytes;
        boolean isWord;
        // target content
        byte[] content;

        Node() {
            children = new Node[BYTE_LENGTH];
            bytes = null;
            isWord = false;
            content = null;
        }
    }
}
