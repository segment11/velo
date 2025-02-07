import com.github.luben.zstd.ZstdCompressCtx
import com.github.luben.zstd.ZstdDictTrainer

def url = this.getClass().getClassLoader().getResource('data/faker-data.csv')
if (url == null) {
    println 'file not exists'
    return
}
def file = new File(url.toURI())
def bytes = new byte[1024 * 1024]
System.arraycopy(file.bytes, 0, bytes, 0, bytes.length)
println 'read file bytes length: ' + bytes.length

int dictSize = 16 * 1024
int sampleCount = 20
int oneSampleBodyLength = 1024 * 50

def trainer = new ZstdDictTrainer(oneSampleBodyLength * sampleCount, dictSize)
sampleCount.times {
    def bb = new byte[oneSampleBodyLength]
    System.arraycopy(bytes, it * oneSampleBodyLength, bb, 0, oneSampleBodyLength)
    trainer.addSample(bb)
}

var beginT = System.currentTimeMillis()
def dictBytes = trainer.trainSamples()
var costT = System.currentTimeMillis() - beginT

println "dict size: ${dictBytes.length}, cost: ${costT}ms"

def compressCtx = new ZstdCompressCtx()
compressCtx.loadDict(dictBytes)

// compress 1M
beginT = System.currentTimeMillis()
def compressedBytes = compressCtx.compress(bytes)
costT = System.currentTimeMillis() - beginT

println "compressed 1M size: ${compressedBytes.length}, cost: ${costT}ms"
println "compress ratio: ${compressedBytes.length / bytes.length}"

// compress 32K
def bytes32 = new byte[32 * 1024]
System.arraycopy(bytes, 0, bytes32, 0, bytes32.length)
def beginT2 = System.nanoTime()
compressedBytes = compressCtx.compress(bytes32)
def costT2 = (System.nanoTime() - beginT2) / 1000

println "compressed 32K size: ${compressedBytes.length}, cost: ${costT2}us"
println "compress ratio: ${compressedBytes.length / bytes32.length}"

/*
read file bytes length: 1048576
dict size: 16384, cost: 35ms
compressed 1M size: 299259, cost: 3ms
compress ratio: 0.28539562225341796875
compressed 32K size: 10094, cost: 304.849us
compress ratio: 0.30804443359375

bellow is C zstd dict train result: (32K compress use much less than java, maybe jni call cost too much)
File read length: 1048576
dict id: 1
train dict 1M cost us avg: 35063
dict buffer length total: 16384
compressed 1M length: 300100
compressed ratio: 0.286198
compressed cost us: 3123
compressed 32K length: 10437
compressed ratio: 0.318512
compressed cost us: 127
 */
