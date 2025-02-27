package io.velo.repl.cluster.watch

import groovy.transform.CompileStatic

/**
 * Represents the status of a single endpoint with a ring buffer of status records.
 */
@CompileStatic
class OneEndpointStatus {

    /**
     * Enum representing possible status states of an endpoint.
     */
    @CompileStatic
    enum Status {
        NOT_LISTEN, PING_OK, PING_FAIL
    }

    /**
     * Record class to encapsulate the status and corresponding timestamp.
     */
    @CompileStatic
    static record StatusRecord(Status status, long timestamp) {
        @Override
        /**
         * Provides a string representation of the StatusRecord with formatted timestamp.
         *
         * @return String formatted as "STATUS=YYYY-MM-DD HH:MM:SS"
         */
        String toString() {
            status.toString() + '=' + new Date(timestamp).format('yyyy-MM-dd HH:mm:ss')
        }
    }

    /**
     * Array to hold the status records in a ring buffer fashion.
     * You can change 5 to 10.
     */
    private final StatusRecord[] statusRecordArray = new StatusRecord[10] // Changed size from 5 to 10

    /**
     * Constructs an instance of OneEndpointStatus with all status records initialized to PING_OK.
     */
    OneEndpointStatus() {
        // init PING_OK
        for (int i = 0; i < statusRecordArray.length; i++) {
            statusRecordArray[i] = new StatusRecord(Status.PING_OK, System.currentTimeMillis())
        }
    }

    @Override
    /**
     * Returns a string representation of all status records in the array.
     *
     * @return String of concatenated status records separated by commas.
     */
    String toString() {
        statusRecordArray.join(',')
    }

    /**
     * Adds a new status record to the end of the array, shifting older records to the left.
     *
     * @param status The new status to add.
     */
    void addStatus(Status status) {
        for (int i = 1; i < statusRecordArray.length; i++) {
            statusRecordArray[i - 1] = statusRecordArray[i]
        }
        statusRecordArray[statusRecordArray.length - 1] = new StatusRecord(status, System.currentTimeMillis())
    }

    /**
     * Checks if any of the status records indicate a PING_OK status.
     *
     * @return true if at least one status is PING_OK, false otherwise.
     */
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