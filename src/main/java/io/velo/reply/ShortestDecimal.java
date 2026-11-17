package io.velo.reply;

import java.math.BigDecimal;

/**
 * The shortest decimal digits that round-trip to a given double, together with the power of ten
 * the digits are scaled by.
 * <p>
 * The value of the decimal is {@code digits * 10^exponent}, the digits carry no sign and no
 * decimal point.
 */
final class ShortestDecimal {

    private final String digits;
    private final int exponent;
    private final boolean negative;

    private ShortestDecimal(String digits, int exponent, boolean negative) {
        this.digits = digits;
        this.exponent = exponent;
        this.negative = negative;
    }

    /**
     * @return the significant decimal digits
     */
    String digits() {
        return digits;
    }

    /**
     * @return the power of ten the digits are scaled by
     */
    int exponent() {
        return exponent;
    }

    /**
     * @return whether the value is negative
     */
    boolean isNegative() {
        return negative;
    }

    /**
     * Computes the shortest decimal that converts back to the given value.
     *
     * @param value the value to represent
     * @return the shortest decimal
     */
    static ShortestDecimal of(double value) {
        // Double.toString() renders the shortest decimal that converts back to the value,
        // the digits are taken from it and the exponent is derived from its scale.
        // stripTrailingZeros() drops the trailing zeroes, so the unscaled value never
        // carries any, and its textual form never carries leading zeroes either.
        var decimal = new BigDecimal(Double.toString(value)).stripTrailingZeros();
        var exponent = -decimal.scale();

        var digits = decimal.unscaledValue().toString();
        // The unscaled value carries the sign, the decimal it belongs to does not.
        var negative = digits.startsWith("-");
        if (negative) {
            digits = digits.substring(1);
        }

        return new ShortestDecimal(digits, exponent, negative);
    }
}
