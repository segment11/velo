package io.velo.persist

import io.velo.ConfForSlot
import io.velo.KeyHash

def n = 10_000_000

//(0..<n).collect {
//    def key = "key:" + it.toString().padLeft(12, '0')
//    BaseCommand.slot(key.bytes, 1).bucketIndex
//}.groupBy {
//    it
//}.each { bucketIndex, list ->
//    println "bucket index: $bucketIndex, size: ${list.size()}"
//}

(0..<n).groupBy {
    def key = "key:" + it.toString().padLeft(12, '0')
    def keyHash = KeyHash.hash(key.bytes)

    def bucketIndex = Math.abs(keyHash % ConfForSlot.global.confBucket.bucketsPerSlot)
    bucketIndex
}.each { bucketIndex, list ->
    println "bucket index: $bucketIndex, size: ${list.size()}"
}