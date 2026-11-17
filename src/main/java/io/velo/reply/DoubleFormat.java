package io.velo.reply;

/**
 * Formats a double the way Redis renders zset scores.
 * <p>
 * Redis uses {@code d2string()} (see {@code src/util.c} in the Redis source): an integral value
 * that fits in a {@code long long} is printed as a plain integer, every other value is printed
 * with the shortest decimal representation, using the scientific notation only when the exponent
 * is out of the plain-decimal range. The notation rules follow Redis' {@code fpconv_dtoa()}
 * ({@code deps/fpconv/fpconv_dtoa.c} in the Redis source).
 */
public final class DoubleFormat {

    /**
     * The largest magnitude Redis converts to a {@code long long} before rendering it as a plain
     * integer, {@code LLONG_MAX / 2} as a double.
     */
    private static final double MAX_PLAIN_INTEGER = 4611686018427387904.0d;

    private DoubleFormat() {
        // Private constructor to prevent instantiation
    }

    /**
     * Formats a double the way Redis renders it in a score reply.
     *
     * @param value the value to format
     * @return the Redis textual representation of the value
     */
    public static String format(double value) {
        if (Double.isNaN(value)) {
            return "nan";
        }
        if (Double.isInfinite(value)) {
            return value > 0 ? "inf" : "-inf";
        }
        if (value == 0.0d) {
            return (1.0d / value) < 0 ? "-0" : "0";
        }

        // An integral value that fits in a long long is printed as a plain integer,
        // exactly like Redis' double2ll() + ll2string() path.
        if (value == Math.rint(value) && Math.abs(value) <= MAX_PLAIN_INTEGER) {
            return Long.toString((long) value);
        }

        var shortest = ShortestDecimal.of(value);
        var rendered = render(shortest.digits(), shortest.exponent());
        return shortest.isNegative() ? "-" + rendered : rendered;
    }

    /**
     * Renders the digits of a decimal with the given exponent, following the notation rules of
     * Redis' {@code emit_digits()}.
     *
     * @param digits   the significant decimal digits, no sign and no decimal point
     * @param exponent the power of ten the digits are scaled by
     * @return the rendered decimal
     */
    private static String render(String digits, int exponent) {
        var length = digits.length();
        // The exponent of the value written as d.dddd.
        var scientificExponent = exponent + length - 1;

        // Plain integer, the digits are followed by the missing zeroes.
        if (exponent >= 0 && Math.abs(scientificExponent) < length + 7) {
            return digits + "0".repeat(exponent);
        }

        // Plain decimal, the point sits inside the digits or in front of them.
        if (exponent < 0 && (exponent > -7 || Math.abs(scientificExponent) < 4)) {
            var offset = length + exponent;
            if (offset <= 0) {
                return "0." + "0".repeat(-offset) + digits;
            }
            return digits.substring(0, offset) + "." + digits.substring(offset);
        }

        // Scientific notation, the exponent always carries a sign and at least two digits.
        var builder = new StringBuilder();
        builder.append(digits.charAt(0));
        if (length > 1) {
            builder.append('.').append(digits, 1, length);
        }
        builder.append('e');
        builder.append(scientificExponent < 0 ? '-' : '+');
        builder.append(Math.abs(scientificExponent));
        return builder.toString();
    }
}
