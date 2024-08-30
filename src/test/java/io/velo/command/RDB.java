package io.velo.command;

import io.netty.buffer.ByteBuf;
import io.netty.buffer.Unpooled;
import io.velo.type.RedisHashKeys;
import io.velo.type.RedisList;
import io.velo.type.RedisZSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class RDB {
    final static int RDB6BitLen = 0;
    final static int RDB14BitLen = 1;
    final static int RDBEncVal = 3;
    final static int RDB32BitLen = 0x80;
    final static int RDB64BitLen = 0x81;
    final static int RDBEncInt8 = 0;
    final static int RDBEncInt16 = 1;
    final static int RDBEncInt32 = 2;
    final static int RDBEncLzf = 3;

    final static int RDBTypeString = 0;
    final static int RDBTypeList = 1;
    final static int RDBTypeSet = 2;
    final static int RDBTypeZSet = 3;
    final static int RDBTypeHash = 4;
    final static int RDBTypeZSet2 = 5;
    final static int RDBTypeModule = 6;
    final static int RDBTypeModule2 = 7;

    // Redis object encoding
    final static int RDBTypeHashZipMap = 9;
    final static int RDBTypeListZipList = 10;
    final static int RDBTypeSetIntSet = 11;
    final static int RDBTypeZSetZipList = 12;
    final static int RDBTypeHashZipList = 13;
    final static int RDBTypeListQuickList = 14;
    final static int RDBTypeStreamListPack = 15;
    final static int RDBTypeHashListPack = 16;
    final static int RDBTypeZSetListPack = 17;
    final static int RDBTypeListQuickList2 = 18;
    final static int RDBTypeStreamListPack2 = 19;
    final static int RDBTypeSetListPack = 20;
    final static int RDBTypeStreamListPack3 = 21;

    final static int QuickListNodeContainerPlain = 1;
    final static int QuickListNodeContainerPacked = 2;

    private final Logger log = LoggerFactory.getLogger(this.getClass());

    private final ByteBuf buf;

    public RDB(ByteBuf buf) {
        this.buf = buf;
    }

    int loadObjectType() {
        var type = buf.readUnsignedByte();
        // 0-5 is the basic type of Redis objects and 9-21 is the encoding type of Redis objects.
        // Redis allow basic is 0-7 and 6/7 is for the module type which we don't support here.
        if ((type >= 0 && type <= 5) || (type >= 9 && type <= 21)) {
            return type;
        }
        throw new IllegalArgumentException("Invalid object type: " + type);
    }

    long loadObjectLen(boolean[] isEncodedArray) {
        var b = buf.readUnsignedByte();
        int type = (b & 0xC0) >> 6;
        long l;

        // why? b == -128
        // fix this, todo
        if (type == 2) {
            type = 3;
        }

        switch (type) {
            case RDBEncVal -> {
                if (isEncodedArray != null) {
                    isEncodedArray[0] = true;
                }
                l = b & 0x3F;
            }
            case RDB6BitLen -> l = b & 0x3F;
            case RDB14BitLen -> l = ((b & 0x3F) << 8) | buf.readUnsignedByte();
            case RDB32BitLen -> l = buf.readUnsignedIntLE();
            case RDB64BitLen -> l = buf.readLong();
            default -> throw new IllegalArgumentException("Unknown length encoding " + type + " in loadObjectLen()");
        }
        return l;
    }

    private static final byte[] EMPTY_STRING_BYTES = "".getBytes();

    private byte[] loadEncodedString() {
        boolean[] isEncodedArray = new boolean[]{false};
        long len = loadObjectLen(isEncodedArray);
        if (isEncodedArray[0]) {
            // For integer type, needs to convert to uint8_t* to avoid signed extension
            if (len == RDBEncInt8) {
                int i = buf.readUnsignedByte();
                return String.valueOf(i).getBytes();
            } else if (len == RDBEncInt16) {
                int b0 = buf.readUnsignedByte();
                int b1 = buf.readUnsignedByte();
                int i = b0 | b1 << 8;
                return String.valueOf(i).getBytes();
            } else if (len == RDBEncInt32) {
                int b0 = buf.readUnsignedByte();
                int b1 = buf.readUnsignedByte();
                int b2 = buf.readUnsignedByte();
                int b3 = buf.readUnsignedByte();
                long i = b0 | b1 << 8 | b2 << 16 | b3 << 24;
                return String.valueOf(i).getBytes();
            } else if (len == RDBEncLzf) {
                return loadLzfString();
            } else {
                throw new IllegalArgumentException("Unknown RDB string encoding type " + len);
            }
        }

        if (len == 0) {
            return EMPTY_STRING_BYTES;
        }

        var dst = new byte[(int) len];
        buf.readBytes(dst);
        return dst;
    }

    private byte[] loadLzfString() {
        var compressedLen = loadObjectLen(null);
        var len = loadObjectLen(null);
        var dst = new byte[(int) len];

        var hb = buf.array();
        int position = buf.readerIndex();

        var n = lzfDecompress(hb, position, (int) compressedLen, dst, (int) len);
        if (n != len) {
            throw new IllegalArgumentException("LZF compressed length mismatch");
        }

        buf.readerIndex(position + (int) compressedLen);
        return dst;
    }

    private int lzfDecompress(byte[] inData, int inPosition, int inLen, byte[] outData, int outLen) {
        int ip = inPosition;
        int op = 0;
        final int inEnd = ip + inLen;
        final int outEnd = op + outLen;

        while (ip < inEnd) {
            // unsigned int
            int ctrl = inData[ip++] & 0xFF;

            if (ctrl < (1 << 5)) {
                ctrl++;

                if (op + ctrl > outEnd) {
                    throw new IllegalArgumentException("E2BIG");
                }

                if (ip + ctrl > inEnd) {
                    throw new IllegalArgumentException("EINVAL");
                }

                switch (ctrl) {
                    case 32:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 31:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 30:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 29:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 28:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 27:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 26:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 25:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 24:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 23:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 22:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 21:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 20:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 19:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 18:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 17:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 16:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 15:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 14:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 13:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 12:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 11:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 10:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 9:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 8:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 7:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 6:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 5:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 4:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 3:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 2:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                    case 1:
                        outData[op++] = (byte) (inData[ip++] & 0xFF);
                }
            } else {
                // unsigned int len = ctrl >> 5;
                int len = (int) ((ctrl >> 5) & 0xFFFFFFFFL);
                // unsigned char *ref = op - ((ctrl & 0x1f) << 8) - 1;
                int refIndex = (int) ((op - ((ctrl & 0x1f) << 8) - 1) & 0xFFFFFFFFL);

                if (ip >= inEnd) {
                    throw new IllegalArgumentException("EINVAL");
                }

                if (len == 7) {
                    len += (inData[ip++] & 0xFF);
                    if (ip >= inEnd) {
                        throw new IllegalArgumentException("EINVAL");
                    }
                }

                refIndex = (int) ((refIndex - (inData[ip++] & 0xFF)) & 0xFFFFFFFFL);

                if (op + len + 2 > outEnd) {
                    throw new IllegalArgumentException("E2BIG");
                }

                if (refIndex < 0) {
                    throw new IllegalArgumentException("EINVAL");
                }

                switch (len) {
                    default:
                        len += 2;

                        if (op >= refIndex + len) {
                            System.arraycopy(outData, refIndex, outData, op, len);
                            op += len;
                        } else {
                            do {
                                outData[op++] = outData[refIndex++];
                            } while (--len > 0);
                        }

                        break;

                    case 9:
                        outData[op++] = outData[refIndex++];
                    case 8:
                        outData[op++] = outData[refIndex++];
                    case 7:
                        outData[op++] = outData[refIndex++];
                    case 6:
                        outData[op++] = outData[refIndex++];
                    case 5:
                        outData[op++] = outData[refIndex++];
                    case 4:
                        outData[op++] = outData[refIndex++];
                    case 3:
                        outData[op++] = outData[refIndex++];
                    case 2:
                        outData[op++] = outData[refIndex++];
                    case 1:
                        outData[op++] = outData[refIndex++];
                    case 0:
                        outData[op++] = outData[refIndex++];
                        outData[op++] = outData[refIndex++];
                }
            }
        }
        return op;
    }

    private void verifyPayloadChecksum() {
        if (buf.readableBytes() < 10) {
            throw new IllegalArgumentException("Invalid payload checksum");
        }
        // last 10 bytes
        buf.markReaderIndex();
        buf.readerIndex(buf.writerIndex() - 10);

        var rdbVersion = buf.readShortLE();
        if (rdbVersion > 11) {
            throw new IllegalArgumentException("Invalid RDB version number");
        }

        long crc = buf.readLongLE();
        // crc64 compare, todo

        buf.resetReaderIndex();
    }

    private RedisHashKeys loadSetObject() {
        var rhk = new RedisHashKeys();

        var len = loadObjectLen(null);
        for (int i = 0; i < len; i++) {
            var memberBytes = loadEncodedString();
            rhk.add(new String(memberBytes));
        }

        return rhk;
    }

    private RedisHashKeys loadSetWithListPack() {
        var encodedBytes = loadEncodedString();
        var bb = Unpooled.wrappedBuffer(encodedBytes);

        var members = RDBListPack.parse(bb);
        var rhk = new RedisHashKeys();
        for (var member : members) {
            rhk.add(member);
        }
        return rhk;
    }

    private RedisZSet loadZSetObject(int type) {
        var rz = new RedisZSet();

        var len = loadObjectLen(null);
        if (len == 0) {
            return rz;
        }

        for (int i = 0; i < len; i++) {
            var memberBytes = loadEncodedString();
            double score;
            if (type == RDBTypeZSet2) {
                score = buf.readDoubleLE();
            } else {
                var b = buf.readUnsignedByte();
                if (255 == b) {
                    score = Double.NEGATIVE_INFINITY;
                } else if (254 == b) {
                    score = Double.POSITIVE_INFINITY;
                } else if (253 == b) {
                    score = Double.NaN;
                } else {
                    var x = new byte[b];
                    buf.readBytes(x);
                    x[b - 1] = '\0';
                    score = Double.parseDouble(new String(x));
                }
            }

            rz.add(score, new String(memberBytes));
        }
        return rz;
    }

    private RedisZSet loadZSetWithListPackOrZipList(int type) {
        var encodedBytes = loadEncodedString();
        var bb = Unpooled.wrappedBuffer(encodedBytes);

        var members = RDBTypeZSetListPack == type ? RDBListPack.parse(bb) : RDBZipList.parse(bb);
        if (members.size() % 2 != 0) {
            throw new IllegalArgumentException("Invalid list pack length");
        }

        var rz = new RedisZSet();
        for (int i = 0; i < members.size(); i += 2) {
            var memberBytes = members.get(i);
            var scoreBytes = members.get(i + 1);
            var score = Double.parseDouble(new String(scoreBytes));

            rz.add(score, new String(memberBytes));
        }
        return rz;
    }

    private RedisList loadListObject() {
        var rl = new RedisList();

        var len = loadObjectLen(null);
        if (len == 0) {
            return rl;
        }

        for (int i = 0; i < len; i++) {
            var memberBytes = loadEncodedString();
            rl.addLast(memberBytes);
        }
        return rl;
    }

    private RedisList loadListWithQuickList(int type) {
        var rl = new RedisList();

        var len = loadObjectLen(null);
        if (len == 0) {
            return rl;
        }

        long container = QuickListNodeContainerPacked;
        for (int i = 0; i < len; i++) {
            if (type == RDBTypeListQuickList2) {
                container = loadObjectLen(null);
                if (container != QuickListNodeContainerPlain && container != QuickListNodeContainerPacked) {
                    throw new IllegalArgumentException("Unknown quicklist node encoding type " + container);
                }
            }

            if (container == QuickListNodeContainerPlain) {
                var memberBytes = loadEncodedString();
                rl.addLast(memberBytes);
                continue;
            }

            var encodedBytes = loadEncodedString();
            var bb = Unpooled.wrappedBuffer(encodedBytes);
            if (type == RDBTypeListQuickList2) {
                var members = RDBListPack.parse(bb);
                for (var member : members) {
                    rl.addLast(member.getBytes());
                }
            } else {
                var members = RDBZipList.parse(bb);
                for (var member : members) {
                    rl.addLast(member.getBytes());
                }
            }
        }
        return rl;
    }

    private RedisHashKeys loadHashObject(Callback callback) {
        var rhk = new RedisHashKeys();

        var len = loadObjectLen(null);
        if (len == 0) {
            return rhk;
        }

        for (int i = 0; i < len; i++) {
            var fieldBytes = loadEncodedString();
            var fieldValueBytes = loadEncodedString();
            var field = new String(fieldBytes);

            callback.onHashFieldValues(field, fieldValueBytes);
            rhk.add(field);
        }
        return rhk;
    }

    private RedisHashKeys loadHashWithListPackOrZipList(int type, Callback callback) {
        var encodedBytes = loadEncodedString();
        var bb = Unpooled.wrappedBuffer(encodedBytes);

        var members = RDBTypeHashListPack == type ? RDBListPack.parse(bb) : RDBZipList.parse(bb);
        if (members.size() % 2 != 0) {
            throw new IllegalArgumentException("Invalid list pack length");
        }

        var rhk = new RedisHashKeys();
        for (int i = 0; i < members.size(); i += 2) {
            var field = members.get(i);
            var fieldValue = members.get(i + 1);

            callback.onHashFieldValues(field, fieldValue.getBytes());
            rhk.add(field);
        }
        return rhk;
    }

    private RedisHashKeys loadHashWithZipMap(Callback callback) {
        var encodedBytes = loadEncodedString();
        var bb = Unpooled.wrappedBuffer(encodedBytes);

        var hashMap = RDBZipMap.parse(bb);

        var rhk = new RedisHashKeys();
        for (var entry : hashMap.entrySet()) {
            var field = entry.getKey();

            callback.onHashFieldValues(field, entry.getValue());
            rhk.add(field);
        }
        return rhk;
    }

    void restore(Callback callback) {
        verifyPayloadChecksum();

        int type = loadObjectType();
        if (RDBTypeString == type) {
            var encodedBytes = loadEncodedString();
            callback.onString(encodedBytes);
        } else if (RDBTypeSet == type) {
            var rhk = loadSetObject();
            callback.onSet(rhk.encode());
        } else if (RDBTypeSetListPack == type) {
            var rhk = loadSetWithListPack();
            callback.onSet(rhk.encode());
        } else if (RDBTypeSetIntSet == type) {
            var encodedBytes = loadEncodedString();
            var bb = Unpooled.wrappedBuffer(encodedBytes);
            var rhk = RDBIntSet.parse(bb);
            callback.onSet(rhk.encode());
        } else if (RDBTypeZSet == type || RDBTypeZSet2 == type) {
            var rz = loadZSetObject(type);
            callback.onZSet(rz.encode());
        } else if (RDBTypeZSetListPack == type || RDBTypeZSetZipList == type) {
            var rz = loadZSetWithListPackOrZipList(type);
            callback.onZSet(rz.encode());
        } else if (RDBTypeList == type) {
            var rl = loadListObject();
            callback.onList(rl.encode());
        } else if (RDBTypeListZipList == type) {
            var encodedBytes = loadEncodedString();
            var bb = Unpooled.wrappedBuffer(encodedBytes);

            var members = RDBZipList.parse(bb);
            var rl = new RedisList();
            for (var member : members) {
                rl.addLast(member.getBytes());
            }
            callback.onList(rl.encode());
        } else if (RDBTypeListQuickList == type || RDBTypeListQuickList2 == type) {
            var rl = loadListWithQuickList(type);
            callback.onList(rl.encode());
        } else if (RDBTypeHash == type) {
            var rhk = loadHashObject(callback);
            callback.onHashKeys(rhk.encode());
        } else if (RDBTypeHashListPack == type || RDBTypeHashZipList == type) {
            var rhk = loadHashWithListPackOrZipList(type, callback);
            callback.onHashKeys(rhk.encode());
        } else if (RDBTypeHashZipMap == type) {
            var rhk = loadHashWithZipMap(callback);
            callback.onHashKeys(rhk.encode());
        } else {
            throw new IllegalArgumentException("Unsupported object type " + type);
        }
    }

    public static interface Callback {
        void onInteger(Integer value);

        void onString(byte[] valueBytes);

        void onList(byte[] encodedBytes);

        void onSet(byte[] encodedBytes);

        void onZSet(byte[] encodedBytes);

        void onHashKeys(byte[] encodedBytes);

        void onHashFieldValues(String field, byte[] valueBytes);
    }
}
