package io.velo.command

import io.velo.persist.ScanCursor
import spock.lang.Specification

class ScanCursorTest extends Specification {
    def 'test base'() {
        given:
        def scanCursor = new ScanCursor((short) 1, (short) 1, (short) 4, (byte) 1)

        when:
        def l = scanCursor.toLong()
        def scanCursor2 = ScanCursor.fromLong(l)
        then:
        scanCursor == scanCursor2
    }
}
