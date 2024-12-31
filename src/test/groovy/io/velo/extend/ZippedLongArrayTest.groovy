package io.velo.extend

import org.apache.lucene.util.RamUsageEstimator
import spock.lang.Specification

class ZippedLongArrayTest extends Specification {
    def 'test all'() {
        given:
        int size = 20 * 1000 * 1000

        long[] longArray = new long[size]
        int[] intArray = new int[size]

        def random = new Random()

        for (int i = 0; i < size; i++) {
            longArray[i] = random.nextLong(size)
            intArray[i] = random.nextInt(size)
        }
        println 'prepared 2000w long and int'

        // sort will be better for compression
//        Arrays.sort(longArray)
//        Arrays.sort(intArray)
//        println 'sorted'

        /*
        sorted:
original ram: 152.6 MB
zipped ram: 38.9 MB
zipped ratio: 0.2547949245
         */
        long originalBytesN = RamUsageEstimator.sizeOf(longArray)
        println 'original ram: ' + RamUsageEstimator.humanReadableUnits(originalBytesN)

        and:
        def zippedLongArr = new ZippedLongArray()
        for (int i = 0; i < longArray.length; i++) {
            zippedLongArr.add(longArray[i])
        }
        println 'shard cursor: ' + zippedLongArr.shardCursor
        println 'shard size: ' + zippedLongArr.shardSize

        long zippedBytesN = zippedLongArr.totalBytesUsed
        println 'zipped ram: ' + RamUsageEstimator.humanReadableUnits(zippedBytesN)
        println 'zipped ratio: ' + (zippedBytesN / originalBytesN)

        expect:
        (0..<10000).every {
            int ix = random.nextInt(size)
            longArray[ix] == zippedLongArr.get(ix)
        }

        when:
        long originalBytesN2 = RamUsageEstimator.sizeOf(intArray)
        println 'original ram: ' + RamUsageEstimator.humanReadableUnits(originalBytesN2)

        def zippedIntArr = new ZippedLongArray(1024 * 1024)
        for (int i = 0; i < intArray.length; i++) {
            zippedIntArr.add(intArray[i])
        }

        /*
        sorted:
original ram: 76.3 MB
zipped ram: 38.9 MB
zipped ratio: 0.5096062106
         */
        long zippedBytesN2 = zippedIntArr.totalBytesUsed
        println 'zipped ram: ' + RamUsageEstimator.humanReadableUnits(zippedBytesN2)
        println 'zipped ratio: ' + (zippedBytesN2 / originalBytesN2)

        then:
        (0..<10000).every {
            int ix = random.nextInt(size)
            intArray[ix] == zippedIntArr.get(ix)
        }

        when:
        boolean exception = false
        try {
            zippedLongArr.add(Long.MAX_VALUE)
        } catch (IllegalArgumentException e) {
            println e.message
            exception = true
        }
        then:
        originalBytesN2
        exception

        when:
        // test performance
        def beginT = System.currentTimeMillis()
        int count = 0
        while (System.currentTimeMillis() < beginT + 5000) {
            int ix = random.nextInt(size)
            assert longArray[ix] == zippedLongArr.get(ix)
            assert intArray[ix] == zippedIntArr.get(ix)
            count++
        }
        int qps = (count / 5).intValue() * 2
        println 'qps: ' + qps
        then:
        qps > 100 * 10000

        when:
        // iterate
        beginT = System.currentTimeMillis();
        while (System.currentTimeMillis() < beginT + 5000) {
            int ix = random.nextInt(size)

            def it = zippedLongArr.getIterator(ix, zippedLongArr.size() - ix)
            int i = ix
            while (it.hasNext()) {
                assert longArray[i++] == it.nextLong()
            }

            def intIt = zippedIntArr.getIterator(ix, zippedIntArr.size() - ix)
            i = ix
            while (intIt.hasNext()) {
                assert intArray[i++] == intIt.nextLong()
            }
        }
        println 'batch fetch done in 5 seconds'
        then:
        1 == 1
    }
}
