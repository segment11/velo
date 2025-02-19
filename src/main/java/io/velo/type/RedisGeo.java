package io.velo.type;

import io.velo.KeyHash;
import org.jetbrains.annotations.VisibleForTesting;

import java.nio.ByteBuffer;
import java.util.HashMap;

/**
 * A class representing a collection of geographic points (latitude and longitude) with associated members.
 * This class provides methods to manipulate the collection, calculate distances, and encode/decode the data.
 */
public class RedisGeo {
    /**
     * A record representing a geographic point with latitude and longitude.
     */
    public record P(double lon, double lat) {
    }

    /**
     * An enum representing the units of distance.
     */
    public enum Unit {
        /**
         * Meters
         */
        M,
        /**
         * Kilometers
         */
        KM,
        /**
         * Miles
         */
        MI,
        /**
         * Feet
         */
        FT,
        /**
         * Unknown unit
         */
        UNKNOWN;

        /**
         * Converts a string to the corresponding Unit enum.
         *
         * @param unitString string representing the unit
         * @return the corresponding Unit enum, or UNKNOWN if the string does not match any known unit
         */
        public static Unit fromString(String unitString) {
            return switch (unitString.toLowerCase()) {
                case "m" -> Unit.M;
                case "km" -> Unit.KM;
                case "mi" -> Unit.MI;
                case "ft" -> Unit.FT;
                default -> Unit.UNKNOWN;
            };
        }

        /**
         * Converts distance from this unit to meters.
         *
         * @param distance distance in the current unit
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
    }

    private final HashMap<String, P> map = new HashMap<>();

    /**
     * Gets the map of members to their geographic points.
     *
     * @return the map of members to their geographic points
     */
    public HashMap<String, P> getMap() {
        return map;
    }

    /**
     * Returns the number of members in the RedisGeo.
     *
     * @return the number of members in the RedisGeo
     */
    public int size() {
        return map.size();
    }

    /**
     * Checks if the RedisGeo is empty.
     *
     * @return true if the RedisGeo is empty, false otherwise
     */
    public boolean isEmpty() {
        return map.isEmpty();
    }

    /**
     * Checks if a member exists in the RedisGeo.
     *
     * @param member the member to check
     * @return true if the member exists, false otherwise
     */
    public boolean contains(String member) {
        return map.containsKey(member);
    }

    /**
     * Gets the geographic point for a given member.
     *
     * @param member the member whose geographic point is to be retrieved
     * @return the geographic point for the given member, or null if the member does not exist
     */
    public P get(String member) {
        return map.get(member);
    }

    /**
     * Adds a member with its geographic point to the RedisGeo.
     *
     * @param member    the member to add
     * @param longitude the longitude of the geographic point
     * @param latitude  the latitude of the geographic point
     */
    public void add(String member, double longitude, double latitude) {
        map.put(member, new P(longitude, latitude));
    }

    /**
     * Removes a member from the RedisGeo.
     *
     * @param key the member to remove
     * @return true if the member was present and was removed, false otherwise
     */
    public boolean remove(String key) {
        return map.remove(key) != null;
    }

    // Earth's radius in meters
    // same as defined in redis geohash_helper.h EARTH_RADIUS_IN_METERS
    private static final double EARTH_RADIUS = 6372797.560856;

    private static final int SCALE_I = 4;

    /**
     * Calculates the distance between two geographic points using the Haversine formula.
     *
     * @param p0 the first geographic point
     * @param p1 the second geographic point
     * @return the distance between the two points in meters
     */
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

    /**
     * Moves the latitude by a specified distance in meters.
     *
     * @param lat              the initial latitude in degrees
     * @param distanceInMeters the distance to move in meters
     * @return the new latitude after moving by the specified distance
     */
    private static double moveLatitude(double lat, double distanceInMeters) {
        return lat + Math.toDegrees(distanceInMeters / EARTH_RADIUS);
    }

    /**
     * Moves the longitude by a specified distance in meters.
     *
     * @param lon              the initial longitude in degrees
     * @param lat              the latitude at which the longitude is measured
     * @param distanceInMeters the distance to move in meters
     * @return the new longitude after moving by the specified distance
     */
    private static double moveLongitude(double lon, double lat, double distanceInMeters) {
        return lon + Math.toDegrees(distanceInMeters / (EARTH_RADIUS * Math.cos(Math.toRadians(lat))));
    }

    /**
     * Checks if a geographic point is within a bounding box.
     *
     * @param p              the geographic point to check
     * @param centerLon      the longitude of the center of the box
     * @param centerLat      the latitude of the center of the box
     * @param widthInMeters  the width of the box in meters
     * @param heightInMeters the height of the box in meters
     * @return true if the point is within the box, false otherwise
     */
    public static boolean isWithinBox(P p, double centerLon, double centerLat, double widthInMeters, double heightInMeters) {
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

    private static final double GEO_LONG_MAX = 180.0;
    private static final double GEO_LONG_MIN = -180.0;
    private static final double GEO_LAT_MAX = 85.05112877980659;
    private static final double GEO_LAT_MIN = -85.05112877980659;

    /**
     * Encodes a geographic point into a geohash.
     *
     * @param lon_min the minimum longitude of the encoding region
     * @param lon_max the maximum longitude of the encoding region
     * @param lat_min the minimum latitude of the encoding region
     * @param lat_max the maximum latitude of the encoding region
     * @param lon     the longitude of the point to encode
     * @param lat     the latitude of the point to encode
     * @param step    the number of bits to use for encoding
     * @return the geohash as a long, or 0 if the point is outside the encoding region
     */
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

    /**
     * Interleaves the bits of two 32-bit integers to form a 64-bit integer.
     *
     * @param xlo the first 32-bit integer
     * @param ylo the second 32-bit integer
     * @return the interleaved 64-bit integer
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
     * Encodes a geographic point into a geohash store value.
     *
     * @param p the geographic point to encode
     * @return the geohash as a long
     */
    public static long hashAsStore(P p) {
        return geohashEncode(GEO_LONG_MIN, GEO_LONG_MAX, GEO_LAT_MIN, GEO_LAT_MAX, p.lon, p.lat, (byte) 26);
    }

    /**
     * Encodes a geographic point into a geohash string.
     *
     * @param p the geographic point to encode
     * @return the geohash as a byte array
     */
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

    /**
     * Encodes the RedisGeo into a byte array.
     *
     * @return the encoded byte array
     */
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

    /**
     * Decodes a byte array into a RedisGeo.
     *
     * @param data the byte array to decode
     * @return the decoded RedisGeo
     */
    public static RedisGeo decode(byte[] data) {
        return decode(data, true);
    }

    /**
     * Decodes a byte array into a RedisGeo with optional CRC32 checking.
     *
     * @param data         the byte array to decode
     * @param doCheckCrc32 whether to perform a CRC32 check on the data
     * @return the decoded RedisGeo
     * @throws IllegalStateException if the CRC32 check fails
     */
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