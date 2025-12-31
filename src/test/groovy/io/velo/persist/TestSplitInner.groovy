package io.velo.persist

import io.velo.SnowFlake

def keysAdd = ['key:000007376834', 'key:000007664986', 'key:000006916734', 'key:000005070371', 'key:000005854480', 'key:000009666120', 'key:000008732287', 'key:000004747121', 'key:000004362004', 'key:000004118887', 'key:000004359051', 'key:000009714845', 'key:000000933269', 'key:000009007287', 'key:000005113840', 'key:000007875298', 'key:000006672948', 'key:00000346917']
def keysExists = ['key:000000403251', 'key:000003470124', 'key:000002917557', 'key:000001106278', 'key:000005564450', 'key:000002044633', 'key:000003469173', 'key:000005615747', 'key:000005737481', 'key:000003063206', 'key:000005282799', 'key:000009261940', 'key:000002807241', 'key:000009189861', 'key:000003914109', 'key:000009247590', 'key:000000794372', 'key:000002799265', 'key:000004294229', 'key:000007155407', 'key:000003654980', 'key:000002336961', 'key:000001838742', 'key:000006634426', 'key:000007896633', 'key:000004452059', 'key:000008524512', 'key:000002003223', 'key:00000379922']

def snowFlake = new SnowFlake(1, 1)
def keyBucket = new KeyBucket((byte) 0, 0, (byte) 0, (byte) 1, null, 0, snowFlake)

for (key in keysExists) {
    keyBucket.put(key, 0L, 0L, 1L, key.bytes)
}
println keyBucket.size

//for (key in keysAdd) {
//    keyBucket.put(key, 0L, 0L, 1L, key.bytes)
//}

def inner = new KeyBucketsInOneWalGroup((byte) 0, 0, null)
inner.splitNumberTmp = [1]
inner.listList << [keyBucket]

List<PersistValueMeta> pvmList = []
for (key in keysAdd) {
    def pvm = new PersistValueMeta()
    pvm.expireAt = 0L
    pvm.seq = 0L
    pvm.key = key
    pvm.keyHash = 0L
    pvm.keyHash32 = 0L
    pvm.bucketIndex = 0
    pvm.extendBytes = key.bytes
    pvmList << pvm
}

inner.putPvmListToTargetBucket(pvmList, 0)