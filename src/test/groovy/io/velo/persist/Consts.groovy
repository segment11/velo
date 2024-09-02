package io.velo.persist

import org.apache.commons.net.telnet.TelnetClient

class Consts {
    static final File persistDir = new File('/tmp/velo-data/test-persist/')
    static final File slotDir = new File(persistDir, 'test-slot')
    static final File slotDir2 = new File(persistDir, 'test-slot2')
    static final File indexDir = new File(persistDir, 'test-index')
    static final File indexDir2 = new File(persistDir, 'test-index2')
    static final File indexWorkerDir = new File(indexDir, 'worker-0')
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
