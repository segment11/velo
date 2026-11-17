package io.velo.reply

import spock.lang.Specification

class DoubleFormatTest extends Specification {

    def 'test integer values'() {
        expect:
        DoubleFormat.format(1.0d) == '1'
        DoubleFormat.format(0.0d) == '0'
        DoubleFormat.format(-1.0d) == '-1'
        DoubleFormat.format(100.0d) == '100'
        DoubleFormat.format(3.0e2) == '300'
        DoubleFormat.format(1.0e15) == '1000000000000000'
        DoubleFormat.format(1.0e18) == '1000000000000000000'
    }

    def 'test fractional values'() {
        expect:
        DoubleFormat.format(1.5d) == '1.5'
        DoubleFormat.format(0.1d) == '0.1'
        DoubleFormat.format(0.30000000000000004d) == '0.30000000000000004'
        DoubleFormat.format(-2.5d) == '-2.5'
    }

    def 'test small and large magnitudes'() {
        expect:
        DoubleFormat.format(1.0e-3) == '0.001'
        DoubleFormat.format(1.0e-4) == '0.0001'
        DoubleFormat.format(0.000001d) == '0.000001'
        DoubleFormat.format(1.0e-7) == '1e-7'
        DoubleFormat.format(1.0e19) == '1e+19'
        DoubleFormat.format(1.0e30) == '1e+30'
        DoubleFormat.format(1.0e100) == '1e+100'
        DoubleFormat.format(1.7976931348623157e308) == '1.7976931348623157e+308'
    }

    def 'test a value too large for a long is written as a plain integer'() {
        expect:
        DoubleFormat.format(1.234567890123e19) == '12345678901230000000'
        DoubleFormat.format(9.8765432109876e18) == '9876543210987600000'
    }

    def 'test non finite values'() {
        expect:
        DoubleFormat.format(Double.NaN) == 'nan'
        DoubleFormat.format(Double.POSITIVE_INFINITY) == 'inf'
        DoubleFormat.format(Double.NEGATIVE_INFINITY) == '-inf'
        DoubleFormat.format(-0.0d) == '-0'
    }
}
