package io.velo.acl

import spock.lang.Specification

class LogRowTest extends Specification {
    def 'test all'() {
        given:
        def logRow = new LogRow()
        logRow.count = 1
        logRow.reason = 'reason'
        logRow.context = 'context'
        logRow.object = 'object'
        logRow.username = 'username'
        logRow.ageSeconds = 1.0
        logRow.clientInfo = 'client-info'
        logRow.entryId = 1
        logRow.timestampCreated = 1
        logRow.timestampLastUpdated = 1

        expect:
        logRow.toReplies().length == 20
    }
}
