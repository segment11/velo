package io.velo.rdb;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.velo.Utils;
import io.velo.command.RDBCallback;
import io.velo.type.RedisHH;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;
import io.velo.type.encode.ListPack;
import io.velo.type.encode.ZipList;

import java.io.File;
import java.nio.ByteBuffer;

public class RDBParser {
    static void setLoadLibraryPath() {
        var dir = new File(Utils.projectPath("/lib"));
        if (dir.exists()) {
            System.setProperty("jna.library.path", dir.getAbsolutePath());
        } else {
            dir = new File(Utils.projectPath("/build/libs/lib"));
            if (dir.exists()) {
                System.setProperty("jna.library.path", dir.getAbsolutePath());
            } else {
                System.out.println("Can't find lib directory");
            }
        }
    }

    static {
        setLoadLibraryPath();
        RedisCrc.crc64Init();
    }

    // Redis RDB type constants
    public static final short RDB_MIN_VERSION = 6;
    public static final short RDB_VERSION = 11;

    private static final int RDB_TYPE_STRING = 0;
    private static final int RDB_TYPE_LIST = 1;
    private static final int RDB_TYPE_SET = 2;
    private static final int RDB_TYPE_ZSET = 3;
    private static final int RDB_TYPE_ZSET2 = 5;
    private static final int RDB_TYPE_HASH = 4;

    private static final int RDB_TYPE_HASH_ZIP_MAP = 9;
    private static final int RDB_TYPE_LIST_ZIP_LIST = 10;
    private static final int RDB_TYPE_SET_INT_SET = 11;
    private static final int RDB_TYPE_ZSET_ZIP_LIST = 12;
    private static final int RDB_TYPE_HASH_ZIP_LIST = 13;
    private static final int RDB_TYPE_LIST_QUICK_LIST = 14;
    private static final int RDB_TYPE_STREAM_LIST_PACK = 15;
    private static final int RDB_TYPE_HASH_LIST_PACK = 16;
    private static final int RDB_TYPE_ZSET_LIST_PACK = 17;
    private static final int RDB_TYPE_LIST_QUICK_LIST2 = 18;
    private static final int RDB_TYPE_STREAM_LIST_PACK2 = 19;
    private static final int RDB_TYPE_SET_LIST_PACK = 20;
    private static final int RDB_TYPE_STREAM_LIST_PACK3 = 21;

    public void readEntry(ByteBuf buf, RDBCallback callback) {
        int type = buf.readUnsignedByte();
        switch (type) {
            case RDB_TYPE_STRING:
                var decoded = decodeRdbString(buf);
                callback.onString(decoded);
                break;
            case RDB_TYPE_LIST:
                var list = new RedisList();
                var listSize = decodeLength(buf, new int[1]);
                for (int i = 0; i < (int) listSize; i++) {
                    list.addLast(decodeRdbString(buf));
                }
                callback.onList(list);
                break;
            case RDB_TYPE_LIST_ZIP_LIST:
                var listZipList = new RedisList();
                var listBuf = Unpooled.wrappedBuffer(decodeRdbString(buf));
                var listZipListSize = listBuf.readShortLE();
                for (int i = 0; i < listZipListSize; i++) {
                    listZipList.addLast(decodeRdbString(listBuf));
                }
                break;
            case RDB_TYPE_LIST_QUICK_LIST, RDB_TYPE_LIST_QUICK_LIST2:
                var listQuickList = new RedisList();
                decodeListQuickList(buf, type, listQuickList);
                callback.onList(listQuickList);
                break;
            case RDB_TYPE_SET:
                var set = new RedisHashKeys();
                var setSize = decodeLength(buf, new int[1]);
                for (int i = 0; i < (int) setSize; i++) {
                    set.add(new String(decodeRdbString(buf)));
                }
                callback.onSet(set);
                break;
            case RDB_TYPE_SET_INT_SET:
                var setIntSet = new RedisHashKeys();
                decodeIntSet(buf, setIntSet);
                callback.onSet(setIntSet);
                break;
            case RDB_TYPE_SET_LIST_PACK:
                var setListPack = new RedisHashKeys();
                decodeSetListPack(buf, setListPack);
                callback.onSet(setListPack);
                break;
            case RDB_TYPE_ZSET, RDB_TYPE_ZSET2:
                var zset = new RedisZSet();
                decodeZSet(buf, type, zset);
                callback.onZSet(zset);
                break;
            case RDB_TYPE_ZSET_ZIP_LIST:
                var zsetZipList = new RedisZSet();
                decodeZSetZipList(buf, zsetZipList);
                callback.onZSet(zsetZipList);
                break;
            case RDB_TYPE_ZSET_LIST_PACK:
                var zsetListPack = new RedisZSet();
                decodeZSetListPack(buf, zsetListPack);
                callback.onZSet(zsetListPack);
                break;
            case RDB_TYPE_HASH:
                var hash = new RedisHH();
                var hashSize = decodeLength(buf, new int[1]);
                for (int i = 0; i < (int) hashSize; i++) {
                    var field = new String(decodeRdbString(buf));
                    var value = decodeRdbString(buf);
                    hash.put(field, value);
                }
                callback.onHash(hash);
                break;
            case RDB_TYPE_HASH_ZIP_MAP:
                var hashZipMap = new RedisHH();
                decodeHashZipMap(buf, hashZipMap);
                callback.onHash(hashZipMap);
                break;
            case RDB_TYPE_HASH_ZIP_LIST:
                var hashZipList = new RedisHH();
                var hashBuf = Unpooled.wrappedBuffer(decodeRdbString(buf));
                short hashZipListSize = hashBuf.readShortLE();
                for (int i = 0; i < (int) hashZipListSize; i += 2) {
                    var field = new String(decodeRdbString(hashBuf));
                    var value = decodeRdbString(hashBuf);
                    hashZipList.put(field, value);
                }
                callback.onHash(hashZipList);
                break;
            case RDB_TYPE_HASH_LIST_PACK:
                var hashListPack = new RedisHH();
                decodeHashListPack(buf, hashListPack);
                callback.onHash(hashListPack);
                break;
            case RDB_TYPE_STREAM_LIST_PACK, RDB_TYPE_STREAM_LIST_PACK2, RDB_TYPE_STREAM_LIST_PACK3:
                throw new IllegalArgumentException("Unsupported type stream yet: " + type);
            default:
                throw new IllegalArgumentException("Unsupported type: " + type);
        }
    }

    // Utility: decode RDB string (length-prefixed, integer, or LZF-compressed)
    private byte[] decodeRdbString(ByteBuf buf) {
        int[] lenType = new int[1];
        long len = decodeLength(buf, lenType);
        if (lenType[0] == 0) { // plain string
            var out = new byte[(int) len];
            buf.readBytes(out);
            return out;
        } else if (lenType[0] == 1) { // integer
            // Integer encoding: 8, 16, 32 bit
            int encType = (int) len;
            if (encType == 0) { // 8 bit
                return Integer.toString(buf.readByte()).getBytes();
            } else if (encType == 1) { // 16 bit
                return Integer.toString(buf.readShortLE()).getBytes();
            } else if (encType == 2) { // 32 bit
                return Integer.toString(buf.readIntLE()).getBytes();
            } else {
                throw new IllegalArgumentException("Unknown integer encoding: " + encType);
            }
        } else if (lenType[0] == 2) { // LZF compressed
            long clen = decodeLength(buf, new int[1]);
            long ulen = decodeLength(buf, new int[1]);
            var cdata = new byte[(int) clen];
            buf.readBytes(cdata);
            var out = new byte[(int) ulen];
            int res = RedisLzf.instance.lzf_decompress(
                    ByteBuffer.wrap(cdata), (int) clen,
                    ByteBuffer.wrap(out), (int) ulen);
            if (res <= 0) {
                throw new IllegalArgumentException("LZF decompress failed");
            }
            return out;
        } else {
            throw new IllegalArgumentException("Unknown RDB string encoding");
        }
    }

    // Utility: decode RDB length (returns value, sets lenType[0]: 0=plain, 1=integer, 2=LZF)
    private long decodeLength(ByteBuf buf, int[] lenType) {
        int first = buf.readUnsignedByte();
        int type = (first & 0xC0) >> 6;
        if (type == 0) { // 6 bit
            lenType[0] = 0;
            return first & 0x3F;
        } else if (type == 1) { // 14 bit
            lenType[0] = 0;
            int second = buf.readUnsignedByte();
            return ((first & 0x3F) << 8) | second;
        } else if (type == 2) { // 32 bit
            lenType[0] = 0;
            return buf.readInt();
        } else { // special encoding
            int enc = first & 0x3F;
            if (enc == 0) { // 8 bit int
                lenType[0] = 1;
                return 0;
            } else if (enc == 1) { // 16 bit int
                lenType[0] = 1;
                return 1;
            } else if (enc == 2) { // 32 bit int
                lenType[0] = 1;
                return 2;
            } else if (enc == 3) { // LZF compressed
                lenType[0] = 2;
                return 0;
            } else {
                throw new IllegalArgumentException("Unknown special encoding: " + enc);
            }
        }
    }


    // Quick list node encoding
    private static final int QuickListNodeContainerPlain = 1;
    private static final int QuickListNodeContainerPacked = 2;

    private void decodeListQuickList(ByteBuf buf, int type, RedisList list) {
        long len = decodeLength(buf, new int[1]);

        long container = QuickListNodeContainerPacked;
        for (int i = 0; i < (int) len; i++) {
            if (type == RDB_TYPE_LIST_QUICK_LIST2) {
                container = decodeLength(buf, new int[1]);
                if (container != QuickListNodeContainerPlain && container != QuickListNodeContainerPacked) {
                    throw new IllegalArgumentException("Unknown quick list node container type: " + container);
                }
            }

            if (container == QuickListNodeContainerPlain) {
                var element = decodeRdbString(buf);
                list.addLast(element);
                continue;
            }

            var encodedBytes = decodeRdbString(buf);
            if (type == RDB_TYPE_LIST_QUICK_LIST2) {
                // list pack
                var listPackBuf = Unpooled.wrappedBuffer(encodedBytes);
                ListPack.decode(listPackBuf, (valueBytes, index) -> {
                    list.addLast(valueBytes);
                });
            } else {
                // zip list
                var zipListBuf = Unpooled.wrappedBuffer(encodedBytes);
                ZipList.decode(zipListBuf, (valueBytes, index) -> {
                    list.addLast(valueBytes);
                });
            }
        }
    }

    private void decodeIntSet(ByteBuf buf, RedisHashKeys set) {
        var setBuf = Unpooled.wrappedBuffer(decodeRdbString(buf));
        int size = setBuf.readableBytes();
        int memberSize = setBuf.readIntLE();
        int len = setBuf.readIntLE();

        if (memberSize == 0) {
            throw new IllegalArgumentException("Invalid intset encoding");
        }

        final int IntSetHeaderSize = 8;
        if (IntSetHeaderSize + memberSize * len != size) {
            throw new IllegalArgumentException("Invalid intset length");
        }

        for (int i = 0; i < len; i++) {
            switch (memberSize) {
                case 2 -> set.add(String.valueOf(setBuf.readUnsignedShortLE()));
                case 4 -> set.add(String.valueOf(setBuf.readUnsignedIntLE()));
                case 8 -> set.add(String.valueOf(setBuf.readLongLE()));
                default -> throw new IllegalArgumentException("Invalid intset encoding");
            }
        }
    }

    private void decodeSetListPack(ByteBuf buf, RedisHashKeys set) {
        var setBuf = Unpooled.wrappedBuffer(decodeRdbString(buf));
        ListPack.decode(setBuf, (valueBytes, index) -> {
            set.add(new String(valueBytes));
        });
    }

    private void decodeZSet(ByteBuf buf, int type, RedisZSet zset) {
        var zsetLen = decodeLength(buf, new int[1]);
        for (int i = 0; i < (int) zsetLen; i++) {
            var member = new String(decodeRdbString(buf));
            double score;
            if (type == RDB_TYPE_ZSET) {
                var lenForDouble = buf.readChar();
                if (lenForDouble == 255) {
                    score = Double.NEGATIVE_INFINITY;
                } else if (lenForDouble == 254) {
                    score = Double.POSITIVE_INFINITY;
                } else if (lenForDouble == 253) {
                    score = Double.NaN;
                } else {
                    var x = new byte[lenForDouble];
                    buf.readBytes(x);
                    score = Double.parseDouble(new String(x));
                }
            } else {
                score = buf.readDouble();
            }
            zset.add(score, member);
        }
    }

    private void decodeZSetZipList(ByteBuf buf, RedisZSet zset) {
        var zsetBuf = Unpooled.wrappedBuffer(decodeRdbString(buf));

        int size = zsetBuf.readShortLE();
        for (int i = 0; i < size; i += 2) {
            var member = new String(decodeRdbString(zsetBuf));
            var doubleStr = new String(decodeRdbString(zsetBuf));
            double score = Double.parseDouble(doubleStr);
            zset.add(score, member);
        }
    }

    private void decodeZSetListPack(ByteBuf buf, RedisZSet zset) {
        var zsetBuf = Unpooled.wrappedBuffer(decodeRdbString(buf));

        final String[] memberArray = new String[1];
        ListPack.decode(zsetBuf, (valueBytes, index) -> {
            if (index % 2 == 0) {
                memberArray[0] = new String(valueBytes);
            } else {
                var scoreStr = new String(valueBytes);
                double score = switch (scoreStr) {
                    case "inf" -> Double.POSITIVE_INFINITY;
                    case "-inf" -> Double.NEGATIVE_INFINITY;
                    case "nan" -> Double.NaN;
                    default -> Double.parseDouble(scoreStr);
                };
                zset.add(score, memberArray[0]);
            }
        });
    }

    private void decodeHashZipMap(ByteBuf buf, RedisHH hash) {
        var hashBuf = Unpooled.wrappedBuffer(decodeRdbString(buf));

        int size = hashBuf.readUnsignedByte();
        if (size == 0xFF) {
            size = hashBuf.readIntLE();
        }

        for (int i = 0; i < size; i++) {
            int fieldLen = hashBuf.readUnsignedByte();
            if (fieldLen == 0xFF) break;

            var field = new byte[fieldLen];
            hashBuf.readBytes(field);

            int valueLen = hashBuf.readUnsignedByte();
            if (valueLen == 0xFF) {
                valueLen = hashBuf.readIntLE();
            }

            var value = new byte[valueLen];
            hashBuf.readBytes(value);

            hash.put(new String(field), value);
        }
    }

    private void decodeHashListPack(ByteBuf buf, RedisHH hash) {
        var hashBuf = Unpooled.wrappedBuffer(decodeRdbString(buf));
        final String[] fieldArray = new String[1];
        ListPack.decode(hashBuf, (valueBytes, index) -> {
            if (index % 2 == 0) {
                fieldArray[0] = new String(valueBytes);
            } else {
                hash.put(fieldArray[0], valueBytes);
            }
        });
    }

    // dump


    private static byte[] writeVersionAndCrc(ByteBuf buf) {
        buf.writeShortLE(RDB_VERSION);
        var crc = RedisCrc.crc64(0, buf.array(), 0, buf.readableBytes());
        buf.writeLongLE(crc);

        var result = new byte[buf.readableBytes()];
        buf.getBytes(0, result);
        return result;
    }

    // string or number
    public static byte[] dumpString(byte[] valueBytes) {
        var buf = Unpooled.buffer();

        buf.writeByte(RDB_TYPE_STRING);
        writeRdbString(buf, valueBytes);

        return writeVersionAndCrc(buf);
    }

    public static byte[] dumpSet(RedisHashKeys rhk) {
        assert rhk != null && rhk.size() > 0;

        var buf = Unpooled.buffer();
        buf.writeByte(RDB_TYPE_SET);
        // Write set size
        writeRdbLength(buf, rhk.size());
        // Write each member
        for (var member : rhk.getSet()) {
            writeRdbString(buf, member.getBytes());
        }

        return writeVersionAndCrc(buf);
    }

    public static byte[] dumpHash(RedisHH rhh) {
        assert rhh != null && rhh.size() > 0;

        var buf = Unpooled.buffer();
        buf.writeByte(RDB_TYPE_HASH);
        // Write hash size
        writeRdbLength(buf, rhh.size());
        // Write each field-value pair
        for (var entry : rhh.getMap().entrySet()) {
            writeRdbString(buf, entry.getKey().getBytes());
            writeRdbString(buf, entry.getValue());
        }

        return writeVersionAndCrc(buf);
    }

    public static byte[] dumpList(RedisList rl) {
        assert rl != null && rl.size() > 0;

        var buf = Unpooled.buffer();
        buf.writeByte(RDB_TYPE_LIST);
        writeRdbLength(buf, rl.size());
        for (var element : rl.getList()) {
            writeRdbString(buf, element);
        }

        return writeVersionAndCrc(buf);
    }

    public static byte[] dumpZSet(RedisZSet rz) {
        assert rz != null && !rz.isEmpty();

        var buf = Unpooled.buffer();
        buf.writeByte(RDB_TYPE_ZSET2);
        writeRdbLength(buf, rz.size());
        for (var entry : rz.getSet()) {
            writeRdbString(buf, entry.member().getBytes());
            buf.writeDouble(entry.score());
        }

        return writeVersionAndCrc(buf);
    }


    // Helper method to write RDB string encoding
    private static void writeRdbString(ByteBuf buf, byte[] data) {
        // Try to encode as integer if possible
        if (data.length <= 11) {
            try {
                var str = new String(data);
                var value = Long.parseLong(str);
                if (value >= -(1 << 7) && value <= (1 << 7) - 1) {
                    // 8-bit integer
                    buf.writeByte((byte) (0xC0));
                    buf.writeByte((byte) value);
                    return;
                } else if (value >= -(1 << 15) && value <= (1 << 15) - 1) {
                    // 16-bit integer
                    buf.writeByte((byte) (0xC0 | 1));
                    buf.writeShortLE((short) value);
                    return;
                } else if (value >= -((long) 1 << 31) && value <= ((long) 1 << 31) - 1) {
                    // 32-bit integer
                    buf.writeByte((byte) (0xC0 | 2));
                    buf.writeIntLE((int) value);
                    return;
                }
            } catch (NumberFormatException e) {
                // Not a number, continue with normal string encoding
            }
        }

        // Normal string encoding
        if (data.length < (1 << 6)) {
            // 6-bit length
            buf.writeByte((byte) (data.length & 0x3F));
        } else if (data.length < (1 << 14)) {
            // 14-bit length
            buf.writeByte((byte) (((data.length >> 8) & 0x3F) | 0x40));
            buf.writeByte((byte) (data.length & 0xFF));
        } else {
            // 32-bit length
            buf.writeByte((byte) 0x80);
            buf.writeInt(data.length);
        }
        buf.writeBytes(data);
    }

    // Helper method to write RDB length encoding
    private static void writeRdbLength(ByteBuf buf, long length) {
        if (length < (1 << 6)) {
            // 6-bit length
            buf.writeByte((byte) (length & 0x3F));
        } else if (length < (1 << 14)) {
            // 14-bit length
            buf.writeByte((byte) (((length >> 8) & 0x3F) | 0x40));
            buf.writeByte((byte) (length & 0xFF));
        } else {
            // 32-bit length
            buf.writeByte((byte) 0x80);
            buf.writeInt((int) length);
        }
    }
}
