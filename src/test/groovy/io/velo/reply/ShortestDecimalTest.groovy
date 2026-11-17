package io.velo.reply

import spock.lang.Specification

class ShortestDecimalTest extends Specification {

    def 'test digits and exponent of a plain decimal'() {
        given:
        def decimal = ShortestDecimal.of(1.5d)

        expect:
        decimal.digits() == '15'
        decimal.exponent() == -1
        !decimal.isNegative()
    }

    def 'test digits and exponent of a value with trailing zeroes'() {
        given:
        def decimal = ShortestDecimal.of(100.0d)

        expect:
        decimal.digits() == '1'
        decimal.exponent() == 2
    }

    def 'test digits and exponent of a value with leading zeroes'() {
        given:
        def decimal = ShortestDecimal.of(0.1d)

        expect:
        decimal.digits() == '1'
        decimal.exponent() == -1
    }

    def 'test digits and exponent of a large magnitude'() {
        given:
        def decimal = ShortestDecimal.of(1.0e30d)

        expect:
        decimal.digits() == '1'
        decimal.exponent() == 30
    }

    def 'test digits and exponent of a small magnitude'() {
        given:
        def decimal = ShortestDecimal.of(1.0e-7d)

        expect:
        decimal.digits() == '1'
        decimal.exponent() == -7
    }

    def 'test the sign is kept apart from the digits'() {
        given:
        def decimal = ShortestDecimal.of(-2.5d)

        expect:
        decimal.digits() == '25'
        decimal.exponent() == -1
        decimal.isNegative()
    }

    def 'test the digits round trip to the value'() {
        expect:
        values.every { value ->
            def decimal = ShortestDecimal.of(value)
            var reconstructed = new BigDecimal(decimal.digits())
                    .movePointRight(decimal.exponent())
            if (decimal.isNegative()) {
                reconstructed = reconstructed.negate()
            }
            reconstructed.doubleValue() == value
        }

        where:
        values << [1.5d, 0.1d, 100.0d, 1.0e30d, 1.0e-7d, 0.001d, 1.0e19d,
                   123456789.123456789d, -2.5d, 0.30000000000000004d, 3.0e2d, 1.0e-4d]
    }
}
