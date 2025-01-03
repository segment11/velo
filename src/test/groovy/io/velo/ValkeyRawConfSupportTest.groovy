package io.velo

import spock.lang.Specification

import java.nio.file.Paths

class ValkeyRawConfSupportTest extends Specification {
    def 'test all'() {
        given:
        def file = Paths.get(ValkeyRawConfSupport.VALKEY_CONF_FILENAME).toFile()
        def file2 = Paths.get(ValkeyRawConfSupport.VALKEY_CONF_FILENAME2).toFile()
        if (file.exists()) {
            file.delete()
        }
        if (file2.exists()) {
            file2.delete()
        }

        when:
        ValkeyRawConfSupport.load()
        then:
        ValkeyRawConfSupport.aclFilename == 'acl.conf'

        when:
        file2.text = '# comment\r\nacl-filename: acl2.conf\r\nacl-pubsub-default: true\r\nxxx: yyy: zzz\r\ntest: test'
        ValkeyRawConfSupport.load()
        then:
        ValkeyRawConfSupport.aclFilename == 'acl2.conf'

        when:
        file.text = 'replica-priority: 1'
        ValkeyRawConfSupport.load()
        then:
        ValkeyRawConfSupport.replicaPriority == 1
    }
}
