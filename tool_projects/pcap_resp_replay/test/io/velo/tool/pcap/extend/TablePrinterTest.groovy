package io.velo.tool.pcap.extend

import spock.lang.Specification

class TablePrinterTest extends Specification {
    def 'test base'() {
        given:
        List<List> table = []
        table << ['name', 'ok count', 'error count']
        table << ['test', 0, 0]

        when:
        TablePrinter.print(table)
        then:
        1 == 1
    }
}
