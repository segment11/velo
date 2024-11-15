package io.velo

import spock.lang.Specification

import java.nio.file.Paths

class ValkeyRawConfSupportTest extends Specification {
    def 'test all'() {
        given:
        var file = Paths.get(ValkeyRawConfSupport.VALKEY_CONF_FILENAME).toFile()
        if (file.exists()) {
            file.delete()
        }

        when:
        ValkeyRawConfSupport.load()
        then:
        ValkeyRawConfSupport.aclFilename == 'acl.conf'

        when:
        file.text = '# comment\r\nacl-filename: acl2.conf\r\nxxx: yyy: zzz\r\ntest: test'
        ValkeyRawConfSupport.load()
        then:
        ValkeyRawConfSupport.aclFilename == 'acl2.conf'
    }
}
