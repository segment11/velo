package io.velo.persist

import io.velo.ConfForSlot
import io.velo.SnowFlake

def dir = new File('/tmp/velo-data/persist')
int slotNumber = 8

ConfForSlot.global = ConfForSlot.c10m

int totalFileCount = 0
//(0..<slotNumber).each { s ->
(4..4).each { s ->
    def slot = s as short
    def slotDir = new File(dir, "slot-${slot}")
    def bigStringDir = new File(slotDir, "big-string")

    if (bigStringDir.exists()) {
        def dirs = bigStringDir.listFiles()
        if (dirs) {
            def raf = new RandomAccessFile(new File(slotDir, 'wal.dat'), 'rw')
            def rafShortValue = new RandomAccessFile(new File(slotDir, 'wal-short-value.dat'), 'rw')
            def snowFlake = new SnowFlake(1, 1)
            def oneSlot = new OneSlot(slot)

            HashMap<Integer, Set<Long>> fileUuidSetByBucketIndex = [:]
            dirs.each { subDir ->
                def files = subDir.listFiles()
                if (files) {
                    Set<Long> set = []
                    fileUuidSetByBucketIndex[subDir.name as int] = set

                    totalFileCount += files.size()
                    for (f in files) {
                        set << (f.name as long)
                    }
                }
            }

            println "--- slot ${slot} ---"
            println '---' * 10
            println "big string file count: ${totalFileCount}"

            int walBigStringCount = 0
            Set<Long> walUuidSet = []
            var walGroupNumber = Wal.calcWalGroupNumber()
            walGroupNumber.times { walGroupIndex ->
                def wal = new Wal(slot, oneSlot, walGroupIndex, raf, rafShortValue, snowFlake)
                wal.lazyReadFromFile()
                walBigStringCount += wal.bigStringFileUuidByKey.size()
                walUuidSet.addAll wal.bigStringFileUuidByKey.values()
            }

            if (totalFileCount != walBigStringCount) {
                println "big string file count not equal to wal big string file count, slot ${slot}, file count ${totalFileCount}, wal big string file count ${walBigStringCount}, ${walUuidSet.size()}"
            }

            fileUuidSetByBucketIndex.each { bucketIndex, set ->
//                def groupIndex = Wal.calcWalGroupIndex(bucketIndex)
//                def wal = new Wal(slot, oneSlot, groupIndex, raf, rafShortValue, snowFlake)
//                wal.lazyReadFromFile()

                for (uuid in set) {
                    if (uuid !in walUuidSet) {
                        println "wal check, slot ${slot}, bucket ${bucketIndex} missing big string file: ${uuid}"
                    }
//                    if (!wal.bigStringFileUuids.contains(uuid)) {
//                        println "wal check, slot ${slot}, bucket ${bucketIndex} missing big string file: ${one.name}"
//                    }
                }
            }
        }
    }
}

println "total big string file count: ${totalFileCount}"
//map.each { k, v ->
//    println "bucket ${k} big string file count: ${v}"
//}