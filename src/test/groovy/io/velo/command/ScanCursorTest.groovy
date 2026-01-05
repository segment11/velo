package io.velo.command

import io.velo.persist.ScanCursor
import spock.lang.Specification

class ScanCursorTest extends Specification {
    def 'test base'() {
        given:
        def scanCursor = new ScanCursor((short) 1, 65536, (short) 1023, (short) 1023, (byte) 1)
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
}
