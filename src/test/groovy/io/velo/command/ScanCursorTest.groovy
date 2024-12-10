package io.velo.command

import io.velo.persist.ScanCursor
import spock.lang.Specification

class ScanCursorTest extends Specification {
    def 'test base'() {
        given:
        def scanCursor = new ScanCursor((short) 1, 65536, Short.MAX_VALUE, (byte) 1)
        println scanCursor
        println ScanCursor.END

        when:
        def l = scanCursor.toLong()
        def scanCursor2 = ScanCursor.fromLong(l)
        println scanCursor2
        then:
        scanCursor == scanCursor2
    }
}
