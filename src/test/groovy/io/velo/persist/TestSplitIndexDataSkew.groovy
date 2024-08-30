package io.velo.persist

import io.velo.ConfForSlot
import io.velo.KeyHash

def map = Mock.prepareKeyHashIndexByKeyBucketList(10_000_000, ConfForSlot.global.confBucket.bucketsPerSlot)

final byte splitNumberToTest = 4
map.each { bucketIndex, list ->
    println "bucket index: $bucketIndex"
    list.groupBy {
        KeyHash.splitIndex(it.v2, splitNumberToTest, bucketIndex)
    }.each { splitIndex, splitList ->
        println "split index: $splitIndex, size: ${splitList.size()}"
    }
}
