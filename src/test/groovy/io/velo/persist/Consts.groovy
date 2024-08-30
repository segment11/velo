package io.velo.persist

import org.apache.commons.net.telnet.TelnetClient

class Consts {
    static final File slotDir = new File('/tmp/velo-data/test-persist/test-slot')
    static final File slotDir2 = new File('/tmp/velo-data/test-persist/test-slot2')
    static final File persistDir = new File('/tmp/velo-data/test-persist/')
    static final File testDir = new File('/tmp/test-velo-data/')

    static {
        slotDir.mkdirs()
    }

    static boolean checkConnectAvailable(String host = 'localhost', int port = 2181) {
        def tc = new TelnetClient(connectTimeout: 500)
        try {
            tc.connect(host, port)
            return true
        } catch (Exception ignored) {
            return false
        } finally {
            tc.disconnect()
        }
    }
}
