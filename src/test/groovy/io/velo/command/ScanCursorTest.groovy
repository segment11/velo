package io.velo.command

import io.velo.persist.ScanCursor
import spock.lang.Specification

class ScanCursorTest extends Specification {
    def 'test base'() {
        given:
        def scanCursor = new ScanCursor((short) 1, 65536, (short) 1023, (short) 1023, (byte) 1, (byte) 1)
        println scanCursor
        println ScanCursor.END

        expect:
        scanCursor.isWalIterateEnd()
        ScanCursor.END.toLong() == 0L

        when:
        def l = scanCursor.toLong()
        def scanCursor2 = ScanCursor.fromLong(l)
        println scanCursor2
        then:
        scanCursor == scanCursor2
    }

    def 'test to long from long with every field'() {
        given:
        // slot 16 bits, wal group index 18 bits, wal skip count 10 bits,
        // bucket index 5 bits, key buckets skip count 10 bits, split index 5 bits
        def scanCursor = new ScanCursor((short) 3, 8191, (short) 1022, (short) 47, (byte) 8, (byte) 31)

        when:
        def decoded = ScanCursor.fromLong(scanCursor.toLong())

        then:
        decoded.slot() == 3
        decoded.walGroupIndex() == 8191
        decoded.walSkipCount() == 1022
        decoded.keyBucketsSkipCount() == 47
        decoded.splitIndex() == 8
        decoded.bucketIndex() == 31
        scanCursor == decoded
    }
}
