package io.velo.repl.cluster.watch

import groovy.transform.CompileStatic

@CompileStatic
class OneEndpointStatus {
    @CompileStatic
    enum Status {
        NOT_LISTEN, PING_OK, PING_FAIL
    }

    @CompileStatic
    static record StatusRecord(Status status, long timestamp) {
        @Override
        String toString() {
            status.toString() + '=' + new Date(timestamp).format('yyyy-MM-dd HH:mm:ss')
        }
    }

    // change 5 -> 10
    private final StatusRecord[] statusRecordArray = new StatusRecord[5]

    OneEndpointStatus() {
        // init PING_OK
        for (int i = 0; i < statusRecordArray.length; i++) {
            statusRecordArray[i] = new StatusRecord(Status.PING_OK, System.currentTimeMillis())
        }
    }

    @Override
    String toString() {
        statusRecordArray.join(',')
    }

    void addStatus(Status status) {
        for (int i = 1; i < statusRecordArray.length; i++) {
            statusRecordArray[i - 1] = statusRecordArray[i]
        }
        statusRecordArray[statusRecordArray.length - 1] = new StatusRecord(status, System.currentTimeMillis())
    }

    boolean isPingOk() {
        // any one is ok
        for (statusRecord in statusRecordArray) {
            if (statusRecord.status() == Status.PING_OK) {
                return true
            }
        }
        return false
    }
}
