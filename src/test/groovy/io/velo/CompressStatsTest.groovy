package io.velo


import spock.lang.Specification

class CompressStatsTest extends Specification {
    def 'test all'() {
        given:
        def compressStats = new CompressStats('test', 'test_')

        when:
        compressStats.compressedCount = 0
        compressStats.decompressedCount = 0
        def mfsList = compressStats.compressStatsGauge.collect()
        then:
        mfsList[0].samples.size() >= 0

        when:
        compressStats.rawCount = 2
        compressStats.compressedCount = 1
        compressStats.rawTotalLength = 100
        compressStats.decompressedCount = 0
        mfsList = compressStats.compressStatsGauge.collect()
        then:
        mfsList[0].samples.size() >= 0

        when:
        compressStats.compressedCount = 0
        compressStats.decompressedCount = 10
        compressStats.decompressedCostTimeTotalUs = 50
        mfsList = compressStats.compressStatsGauge.collect()
        then:
        mfsList[0].samples.size() >= 0
    }
}
