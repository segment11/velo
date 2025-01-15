package io.velo.type;

import io.velo.KeyHash;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.HashMap;

public class RedisGeo {
    public record P(double lon, double lat) {

    }

    private final HashMap<String, P> map = new HashMap<>();

    public int size() {
        return map.size();
    }

    public boolean isEmpty() {
        return map.isEmpty();
    }

    public boolean contains(String member) {
        return map.containsKey(member);
    }

    public P get(String member) {
        return map.get(member);
    }

    public void add(String member, double longitude, double latitude) {
        map.put(member, new P(longitude, latitude));
    }

    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    // Earth's radius in meters
    // same as define in redis geohash_helper.h EARTH_RADIUS_IN_METERS
    private static final double EARTH_RADIUS = 6372797.560856;

    private static final int SCALE_I = 4;

    public static double distance(P p0, P p1) {
        // Convert latitude and longitude from degrees to radians
        double lat1Rad = Math.toRadians(p0.lat);
        double lon1Rad = Math.toRadians(p0.lon);
        double lat2Rad = Math.toRadians(p1.lat);
        double lon2Rad = Math.toRadians(p1.lon);

        // Haversine formula
        double diffLat = lat2Rad - lat1Rad;
        double diffLon = lon2Rad - lon1Rad;
        double a = Math.sin(diffLat / 2) * Math.sin(diffLat / 2) +
                Math.cos(lat1Rad) * Math.cos(lat2Rad) *
                        Math.sin(diffLon / 2) * Math.sin(diffLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        double r = EARTH_RADIUS * c;
        // scale to 4 decimal places
        return Math.round(r * Math.pow(10, SCALE_I)) / Math.pow(10, SCALE_I);
    }

    private static final double GEO_LONG_MAX = 180.0;
    private static final double GEO_LONG_MIN = -180.0;
    private static final double GEO_LAT_MAX = 85.05112877980659;
    private static final double GEO_LAT_MIN = -85.05112877980659;

    @VisibleForTesting
    static long geohashEncode(double lon_min, double lon_max, double lat_min, double lat_max,
                              double lon, double lat, byte step) {
        if (lon > GEO_LONG_MAX || lon < GEO_LONG_MIN || lat > GEO_LAT_MAX || lat < GEO_LAT_MIN) {
            return 0;
        }

        if (lon < lon_min || lon > lon_max || lat < lat_min || lat > lat_max) {
            return 0;
        }

        double lat_offset = (lat - lat_min) / (lat_max - lat_min);
        double lon_offset = (lon - lon_min) / (lon_max - lon_min);

        lat_offset *= (1L << step);
        lon_offset *= (1L << step);

        return interleave64((int) lat_offset, (int) lon_offset);
    }

    private static final byte[] geoalphabet = "0123456789bcdefghjkmnpqrstuvwxyz".getBytes();

    private static final long[] B = {0x5555555555555555L, 0x3333333333333333L, 0x0F0F0F0F0F0F0F0FL,
            0x00FF00FF00FF00FFL, 0x0000FFFF0000FFFFL};

    private static final int[] S = {1, 2, 4, 8, 16};

    private static long interleave64(int xlo, int ylo) {
        long x = xlo;
        long y = ylo;

        x = (x | (x << S[4])) & B[4];
        y = (y | (y << S[4])) & B[4];

        x = (x | (x << S[3])) & B[3];
        y = (y | (y << S[3])) & B[3];

        x = (x | (x << S[2])) & B[2];
        y = (y | (y << S[2])) & B[2];

        x = (x | (x << S[1])) & B[1];
        y = (y | (y << S[1])) & B[1];

        x = (x | (x << S[0])) & B[0];
        y = (y | (y << S[0])) & B[0];

        return x | (y << 1);
    }

    public static byte[] hash(P p) {
        var l = geohashEncode(-180, 180, -90, 90, p.lon, p.lat, (byte) 26);

        var buf = new byte[11];
        int i;
        for (i = 0; i < 11; i++) {
            int idx;
            if (i == 10) {
                idx = 0;
            } else {
                idx = (int) ((l >> (52 - ((i + 1) * 5))) & 0x1f);
            }
            buf[i] = geoalphabet[idx];
        }
        return buf;
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
            var p = entry.getValue();
            buffer.putShort((short) member.getBytes().length);
            buffer.put(member.getBytes());
            buffer.putDouble(p.lon);
            buffer.putDouble(p.lat);
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
            var lon = buffer.getDouble();
            var lat = buffer.getDouble();
            r.map.put(new String(memberBytes), new P(lon, lat));
        }
        return r;
    }
}
