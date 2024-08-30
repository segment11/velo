package io.velo

import io.activej.config.Config
import org.apache.commons.io.FileUtils
import spock.lang.Specification

class ConfVolumeDirsForSlotTest extends Specification {
    def 'test init'() {
        given:
        def persistConfigBlank = Config.create()
        def persistConfig = Config.create().with('volumeDirsBySlot',
                '/tmp/data0:0-31,/tmp/data1:32-63,/tmp/data2:64-95,/tmp/data3:96-127')

        when:
        new File('/tmp/data0').mkdir()
        new File('/tmp/data1').mkdir()
        new File('/tmp/data2').mkdir()
        new File('/tmp/data3').mkdir()
        ConfVolumeDirsForSlot.initFromConfig(persistConfigBlank, (short) 128)
        ConfVolumeDirsForSlot.initFromConfig(persistConfig, (short) 128)
        then:
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 0) == '/tmp/data0'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 31) == '/tmp/data0'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 32) == '/tmp/data1'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 63) == '/tmp/data1'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 64) == '/tmp/data2'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 95) == '/tmp/data2'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 96) == '/tmp/data3'
        ConfVolumeDirsForSlot.getVolumeDirBySlot((byte) 127) == '/tmp/data3'
    }

    def 'test exception'() {
        when:
        boolean exception = false
        try {
            ConfVolumeDirsForSlot.initFromConfig(Config.create().with('volumeDirsBySlot', '/tmp/data0:0-31:xxx'), (short) 128)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            ConfVolumeDirsForSlot.initFromConfig(Config.create().with('volumeDirsBySlot', '/tmp/data0-not-exists-dir:0-31'), (short) 128)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        def dir = new File('/tmp/data0-not-exists-dir')
        FileUtils.touch(dir)
        try {
            ConfVolumeDirsForSlot.initFromConfig(Config.create().with('volumeDirsBySlot', '/tmp/data0-not-exists-dir:0-31'), (short) 128)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            ConfVolumeDirsForSlot.initFromConfig(Config.create().with('volumeDirsBySlot', '/tmp/data0:0-31-x'), (short) 128)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            ConfVolumeDirsForSlot.initFromConfig(Config.create().with('volumeDirsBySlot', '/tmp/data0:128-127'), (short) 128)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            ConfVolumeDirsForSlot.initFromConfig(Config.create().with('volumeDirsBySlot', '/tmp/data0:0-128'), (short) 128)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        dir.delete()
    }
}
