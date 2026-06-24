package io.velo.type;

import io.velo.KeyHash;
import io.velo.persist.Wal;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * Geographic points collection with distance calculation and encoding.
 */
public class RedisGeo {
    /**
     * Geographic point with longitude and latitude.
     */
    public record P(double lon, double lat) {
    }

    /**
     * Distance units for geographic calculations.
     */
    public enum Unit {
        /** Meters */
        M,
        /** Kilometers */
        KM,
        /** Miles */
        MI,
        /** Feet */
        FT,
        /** Unknown unit */
        UNKNOWN;

        /**
         * @param unitString string representing the unit
         * @return corresponding Unit or UNKNOWN if not found
         */
        public static Unit fromString(@NotNull String unitString) {
            return switch (unitString.toLowerCase()) {
                case "m" -> Unit.M;
                case "km" -> Unit.KM;
                case "mi" -> Unit.MI;
                case "ft" -> Unit.FT;
                default -> Unit.UNKNOWN;
            };
        }

        /**
         * @param distance distance in this unit
         * @return distance in meters
         */
        public double toMeters(double distance) {
            return switch (this) {
                case M -> distance;
                case KM -> distance * 1000;
                case MI -> distance * 1609.34;
                case FT -> distance * 0.3048;
                case UNKNOWN -> distance;
            };
        }

        /**
         * @param meters distance in meters
         * @return distance in this unit
         */
        public double fromMeters(double meters) {
            return switch (this) {
                case M -> meters;
                case KM -> meters / 1000;
                case MI -> meters / 1609.34;
                case FT -> meters / 0.3048;
                case UNKNOWN -> meters;
            };
        }
    }

    private final HashMap<String, P> map = new HashMap<>();

    /** @return map of members to their geographic points */
    public HashMap<String, P> getMap() {
        return map;
    }

    /** @return number of members */
    public int size() {
        return map.size();
    }

    /** @return true if empty */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * @param member the member to check
     * @return true if member exists
     */
    public boolean contains(String member) {
        return map.containsKey(member);
    }

    /**
     * @param member the member to retrieve
     * @return geographic point or null if not found
     */
    public P get(String member) {
        return map.get(member);
    }

    /**
     * @param member    the member to add
     * @param longitude longitude coordinate
     * @param latitude  latitude coordinate
     */
    public void add(String member, double longitude, double latitude) {
        if (longitude > GEO_LONG_MAX || longitude < GEO_LONG_MIN) {
            throw new IllegalArgumentException(
                    "Longitude must be between " + GEO_LONG_MIN + " and " + GEO_LONG_MAX);
        }
        if (latitude > GEO_LAT_MAX || latitude < GEO_LAT_MIN) {
            throw new IllegalArgumentException(
                    "Latitude must be between " + GEO_LAT_MIN + " and " + GEO_LAT_MAX);
        }
        map.put(member, new P(longitude, latitude));
    }

    /**
     * @param key the member to remove
     * @return true if removed
     */
    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    private static final double EARTH_RADIUS = 6372797.560856;

    private static final int SCALE_I = 4;

    /**
     * @param p0 the first geographic point
     * @param p1 the second geographic point
     * @return distance in meters using Haversine formula
     */
    public static double distance(P p0, P p1) {
        double lat1Rad = Math.toRadians(p0.lat);
        double lon1Rad = Math.toRadians(p0.lon);
        double lat2Rad = Math.toRadians(p1.lat);
        double lon2Rad = Math.toRadians(p1.lon);

        double diffLat = lat2Rad - lat1Rad;
        double diffLon = lon2Rad - lon1Rad;
        double a = Math.sin(diffLat / 2) * Math.sin(diffLat / 2) +
                Math.cos(lat1Rad) * Math.cos(lat2Rad) *
                        Math.sin(diffLon / 2) * Math.sin(diffLon / 2);
        double c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));

        double r = EARTH_RADIUS * c;
        return Math.round(r * Math.pow(10, SCALE_I)) / Math.pow(10, SCALE_I);
    }

    private static double moveLatitude(double lat, double distanceInMeters) {
        return lat + Math.toDegrees(distanceInMeters / EARTH_RADIUS);
    }

    private static double moveLongitude(double lon, double lat, double distanceInMeters) {
        return lon + Math.toDegrees(distanceInMeters / (EARTH_RADIUS * Math.cos(Math.toRadians(lat))));
    }

    /**
     * @param p              geographic point to check
     * @param centerLon      center longitude
     * @param centerLat      center latitude
     * @param widthInMeters  box width
     * @param heightInMeters box height
     * @return true if point is within bounding box
     */
    public static boolean isWithinBox(P p, double centerLon, double centerLat,
                                      double widthInMeters, double heightInMeters) {
        double halfWidth = widthInMeters / 2;
        double halfHeight = heightInMeters / 2;

        double northLat = moveLatitude(centerLat, halfHeight);
        if (p.lat > northLat) {
            return false;
        }
        double southLat = moveLatitude(centerLat, -halfHeight);
        if (p.lat < southLat) {
            return false;
        }

        double eastLon = moveLongitude(centerLon, centerLat, halfWidth);
        if (p.lon > eastLon) {
            return false;
        }
        double westLon = moveLongitude(centerLon, centerLat, -halfWidth);
        return p.lon >= westLon;
    }

    /** Maximum valid longitude for geo encoding. */
    public static final double GEO_LONG_MAX = 180.0;
    /** Minimum valid longitude for geo encoding. */
    public static final double GEO_LONG_MIN = -180.0;
    /** Maximum valid latitude for geo encoding (Web Mercator limit). */
    public static final double GEO_LAT_MAX = 85.05112877980659;
    /** Minimum valid latitude for geo encoding (Web Mercator limit). */
    public static final double GEO_LAT_MIN = -85.05112877980659;

    /**
     * @param lon_min minimum longitude of encoding region
     * @param lon_max maximum longitude of encoding region
     * @param lat_min minimum latitude of encoding region
     * @param lat_max maximum latitude of encoding region
     * @param lon     longitude to encode
     * @param lat     latitude to encode
     * @param step    number of bits for encoding
     * @return geohash as long, or 0 if outside region
     */
    @VisibleForTesting
    static long geohashEncode(double lon_min, double lon_max, double lat_min, double lat_max,
                              double lon, double lat, byte step) {
        if (lon > GEO_LONG_MAX || lon < GEO_LONG_MIN ||
                lat > GEO_LAT_MAX || lat < GEO_LAT_MIN) {
            return 0;
        }

        if (lon < lon_min || lon > lon_max || lat < lat_min || lat > lat_max) {
            return 0;
        }

        if (lat_max == lat_min || lon_max == lon_min) {
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

    /**
     * @param xlo first 32-bit integer
     * @param ylo second 32-bit integer
     * @return interleaved 64-bit integer
     */
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

    /**
     * @param p geographic point
     * @return geohash as long
     */
    public static long hashAsStore(P p) {
        return geohashEncode(GEO_LONG_MIN, GEO_LONG_MAX, GEO_LAT_MIN, GEO_LAT_MAX,
                p.lon, p.lat, (byte) 26);
    }

    /**
     * @param p geographic point
     * @return geohash as byte array
     */
    public static byte[] hash(P p) {
        var l = geohashEncode(-180, 180, -90, 90, p.lon, p.lat, (byte) 26);

        var bytes = new byte[11];
        int i;
        for (i = 0; i < 11; i++) {
            int idx;
            if (i == 10) {
                idx = 0;
            } else {
                idx = (int) ((l >> (52 - ((i + 1) * 5))) & 0x1f);
            }
            bytes[i] = geoalphabet[idx];
        }
        return bytes;
    }

    /** Header length: size short + body length int + crc int */
    @VisibleForTesting
    static final int HEADER_LENGTH = 2 + 4 + 4;

    /**
     * @return encoded byte array
     */
    public byte[] encode() {
        int bodyBytesLength = 0;
        for (var entry : map.entrySet()) {
            var member = entry.getKey();
            bodyBytesLength += 2 + Wal.keyBytes(member).length + 16;
        }

        int size = map.size();
        if (size > Short.MAX_VALUE) {
            throw new IllegalStateException("Geo size " + size + " exceeds Short.MAX_VALUE");
        }

        var buffer = ByteBuffer.allocate(bodyBytesLength + HEADER_LENGTH);
        buffer.putShort((short) size);
        buffer.putInt(bodyBytesLength);
        buffer.putInt(0);
        for (var entry : map.entrySet()) {
            var member = entry.getKey();
            var memberBytes = Wal.keyBytes(member);
            if (memberBytes.length > Short.MAX_VALUE) {
                throw new IllegalStateException(
                        "Geo member length " + memberBytes.length + " exceeds Short.MAX_VALUE");
            }
            var p = entry.getValue();
            buffer.putShort((short) memberBytes.length);
            buffer.put(memberBytes);
            buffer.putDouble(p.lon);
            buffer.putDouble(p.lat);
        }

        int crc = 0;
        if (bodyBytesLength > 0) {
            var hb = buffer.array();
            crc = KeyHash.hash32Offset(hb, HEADER_LENGTH, hb.length - HEADER_LENGTH);
            buffer.putInt(HEADER_LENGTH - 4, crc);
        }

        return buffer.array();
    }

    /**
     * @param data the byte array to decode
     * @return decoded RedisGeo
     */
    public static RedisGeo decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * @param data         the byte array to decode
     * @param doCheckCrc32 whether to perform CRC32 check
     * @return decoded RedisGeo
     */
    public static RedisGeo decode(byte[] data, boolean doCheckCrc32) {
        var r = new RedisGeo();

        var buffer = ByteBuffer.wrap(data);
        var size = buffer.getShort();
        var bodyBytesLength = buffer.getInt();
        var crc = buffer.getInt();

        if (size > 0 && doCheckCrc32) {
            int crcCompare = KeyHash.hash32Offset(
                    buffer.array(), buffer.position(), buffer.remaining());
            if (crc != crcCompare) {
                throw new IllegalStateException("Crc check failed");
            }
        }

        for (int i = 0; i < size; i++) {
            int memberLength = buffer.getShort();
            if (memberLength <= 0) {
                throw new IllegalStateException(
                        "Invalid member length: " + memberLength + ", expected > 0");
            }
            if (memberLength + 16 > buffer.remaining()) {
                throw new IllegalStateException(
                        "Invalid member length: " + memberLength + ", exceeds remaining buffer");
            }
            var memberBytes = new byte[memberLength];
            buffer.get(memberBytes);
            var lon = buffer.getDouble();
            var lat = buffer.getDouble();
            if (lon > GEO_LONG_MAX || lon < GEO_LONG_MIN) {
                throw new IllegalArgumentException(
                        "Longitude must be between " + GEO_LONG_MIN + " and " + GEO_LONG_MAX);
            }
            if (lat > GEO_LAT_MAX || lat < GEO_LAT_MIN) {
                throw new IllegalArgumentException(
                        "Latitude must be between " + GEO_LAT_MIN + " and " + GEO_LAT_MAX);
            }
            r.map.put(Wal.keyString(memberBytes), new P(lon, lat));
        }
        return r;
    }
}
