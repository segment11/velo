package io.velo.extend

import org.apache.commons.io.FileUtils
import spock.lang.Specification

class CountServiceTest extends Specification {
    def 'test get and increase'() {
        given:
        final int initBytesArraySize = 1024
        def countService = new CountService(initBytesArraySize, false)
        def countServiceCompress = new CountService(initBytesArraySize, true)

        and:
        final int keyCount = 100 * initBytesArraySize
        def keyBytesList = (0..<keyCount).collect {
            ('key:' + it.toString().padLeft(12, '0')).bytes
        }
        println 'init key list size: ' + keyBytesList.size()
        final byte[] keyBytesPlus1 = ('key:' + keyCount.toString().padLeft(12, '0')).bytes

        expect:
        keyBytesList[0..<100].every {
            countService.get(it) == 0
            countServiceCompress.get(it) == 0
        }
        countService.getEncodedCompressRatio(0) == 1.0d
        countServiceCompress.getEncodedCompressRatio(0) == 0.0d

        when:
        final int oneKeyCountEstimate = 1_000
        def random = new Random()
        def beginT = System.currentTimeMillis()
        keyBytesList.each {
            countServiceCompress.increase(it, random.nextInt(oneKeyCountEstimate))
        }
        println 'increase1 use compress cost time: ' + (System.currentTimeMillis() - beginT) + 'ms'
        beginT = System.currentTimeMillis()
        keyBytesList.each {
            countServiceCompress.increase(it, random.nextInt(oneKeyCountEstimate))
        }
        println 'increase2 use compress cost time: ' + (System.currentTimeMillis() - beginT) + 'ms'
        10.times {
            println countServiceCompress.getEncodedCompressRatio(it)
        }
        // no compress increase
        beginT = System.currentTimeMillis()
        keyBytesList.each {
            countService.increase(it, random.nextInt(oneKeyCountEstimate))
        }
        println 'increase1 no compress cost time: ' + (System.currentTimeMillis() - beginT) + 'ms'
        beginT = System.currentTimeMillis()
        keyBytesList.each {
            countService.increase(it, random.nextInt(oneKeyCountEstimate))
        }
        println 'increase2 no compress cost time: ' + (System.currentTimeMillis() - beginT) + 'ms'
        then:
        keyBytesList.every {
            countService.get(it) > 0
            countServiceCompress.get(it) > 0
        }
        countService.get(keyBytesPlus1) == 0
        countServiceCompress.get(keyBytesPlus1) == 0

        when:
        boolean exception = false
        try {
            def countService1 = new CountService(0, false)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            def countService1 = new CountService(CountService.MAX_INIT_BYTES_ARRAY_SIZE + 1, true)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception

        when:
        exception = false
        try {
            def countService1 = new CountService(1, true)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        exception
    }

    def 'test save and load'() {
        given:
        final int initBytesArraySize = 1024
        def countService = new CountService(initBytesArraySize, false)

        and:
        countService.slot = (byte) 0
        def persistDir = new File('/tmp/test_count_service_persist')
        def persistDirNotExist = new File('/tmp/test_count_service_persist2')
        if (!persistDir.exists()) {
            FileUtils.forceMkdir(persistDir)
        }
        if (persistDirNotExist.exists()) {
            FileUtils.deleteDirectory(persistDirNotExist)
        }
        countService.persistDir = persistDirNotExist
        countService.loadFromLastSavedFile()
        println countService.estimate(new StringBuilder())
        println countService.collect()

        expect:
        countService.slot == 0
        countService.persistDir == persistDirNotExist

        when:
        countService.persistDir = persistDir
        countService.writeToSavedFile()
        def countService1 = new CountService(initBytesArraySize, false)
        countService1.slot = (byte) 0
        countService1.persistDir = persistDir
        // no data saved
        countService1.loadFromLastSavedFile()
        then:
        countService1.estimate(new StringBuilder()) == 0

        when:
        final int keyCount = 100 * initBytesArraySize
        def keyBytesList = (0..<keyCount).collect {
            ('key:' + it.toString().padLeft(12, '0')).bytes
        }
        println 'init key list size: ' + keyBytesList.size()

        int oneKeyCountEstimate = 1_000
        def random = new Random()
        keyBytesList.each {
            countService.increase(it, random.nextInt(oneKeyCountEstimate))
        }
        def s = countService.estimate(new StringBuilder())
        countService.writeToSavedFile()
        def countService2 = new CountService(initBytesArraySize, false)
        countService2.slot = (byte) 0
        countService2.persistDir = persistDir
        countService2.loadFromLastSavedFile()
        then:
        s > 0
        keyBytesList.every {
            countService.get(it) == countService2.get(it)
        }

        when:
        def f = countService2.lazyReadFromFile()
        then:
        f.get()

        cleanup:
        new File(persistDir, CountService.SAVE_FILE_NAME_PREFIX + '0.dat').delete()
    }
}
