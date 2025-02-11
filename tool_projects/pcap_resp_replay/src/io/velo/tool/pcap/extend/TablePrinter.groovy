package io.velo.tool.pcap.extend

class TablePrinter {
    static void print(List<List<Object>> table) {
        def first = table[0]

        List<Integer> maxLengths = []
        for (i in 0..<first.size()) {
            maxLengths << table.collect { it[i]?.toString() }.max { it == null ? 0 : it.length() }.length()
        }

        def sbRow = new StringBuilder()
        for (maxLength in maxLengths) {
            sbRow << '+' << ('-' * (maxLength + 2))
        }
        sbRow << '+'
        def rowSplit = sbRow.toString()

        def sb = new StringBuilder()
        sb << rowSplit.blue()
        sb << '\n'

        table.eachWithIndex { List<Object> row, int j ->
            sb << '|'.green()

            for (i in 0..<first.size()) {
                def maxLength = maxLengths[i]
                def val = row[i]?.toString()
                def valStr = val == null ? 'null' : val
                def valLine = ' ' + valStr.padRight(maxLength + 1, ' ')
                sb << (j == 0 ? valLine.orange() : valLine)
                sb << '|'.green()
            }
            sb << '\n'
            sb << ((j == 0 || j == table.size() - 1) ? rowSplit.blue() : rowSplit)
            sb << '\n'
        }
        println sb.toString()
    }
}
