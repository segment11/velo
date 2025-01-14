package io.velo.type;

import io.velo.KeyHash;
import org.jetbrains.annotations.VisibleForTesting;
import org.locationtech.jts.geom.Coordinate;
import org.locationtech.jts.geom.GeometryFactory;
import org.locationtech.jts.geom.Point;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class RedisGeo {
    private static final GeometryFactory geometryFactory = new GeometryFactory();

    private final HashMap<String, Point> map = new HashMap<>();

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean contains(String member) {
        return map.containsKey(member);
    }

    public Point get(String member) {
        return map.get(member);
    }

    public void add(String member, double longitude, double latitude) {
        // x -> latitude, y -> longitude
        var point = geometryFactory.createPoint(new Coordinate(latitude, longitude));
        map.put(member, point);
    }

    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    @VisibleForTesting
    // size short + body bytes length int + crc int
    static final int HEADER_LENGTH = 2 + 4 + 4;

    public byte[] encode() {
        int bodyBytesLength = 0;
        for (var entry : map.entrySet()) {
            var member = entry.getKey();
            // longitude and latitude use 8 bytes
            bodyBytesLength += 2 + member.getBytes().length + 16;
        }

        short size = (short) map.size();

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort(size);
        buffer.putInt(bodyBytesLength);
        // tmp crc
        buffer.putInt(0);
        for (var entry : map.entrySet()) {
            var member = entry.getKey();
            var value = entry.getValue();
            buffer.putShort((short) member.getBytes().length);
            buffer.put(member.getBytes());
            buffer.putDouble(value.getX());
            buffer.putDouble(value.getY());
        }

        // crc
        int crc = 0;
        if (bodyBytesLength > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        return buffer.array();
    }

    public static RedisGeo decode(byte[] data) {
        return decode(data, true);
    }

    public static RedisGeo decode(byte[] data, boolean doCheckCrc32) {
        var r = new RedisGeo();

        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        // check crc
        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("Crc check failed");
            }
        }

        for (int i = 0; i < size; i++) {
            int memberLength = buffer.getShort();
            var memberBytes = new byte[memberLength];
            buffer.get(memberBytes);
            var x = buffer.getDouble();
            var y = buffer.getDouble();
            r.map.put(new String(memberBytes), geometryFactory.createPoint(new Coordinate(x, y)));
        }
        return r;
    }
}
