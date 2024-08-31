package io.velo

import com.github.luben.zstd.Zstd
import spock.lang.Specification

class TrainSampleJobTest extends Specification {

    def 'test key prefix or suffix'() {
        given:
        TrainSampleJob.dictKeyPrefixEndIndex = 6

        TrainSampleJob.addKeyPrefixGroupIfNotExist 'key:'
        TrainSampleJob.addKeyPrefixGroupIfNotExist 'key2:'
        TrainSampleJob.keyPrefixOrSuffixGroup('test.xxx.yyy') == 'test.xxx'
        TrainSampleJob.keyPrefixOrSuffixGroupList = ['key:', 'key2:']
        TrainSampleJob.addKeyPrefixGroupIfNotExist 'key:'

        expect:
        TrainSampleJob.keyPrefixOrSuffixGroup('key:1234567890') == 'key:'
        TrainSampleJob.keyPrefixOrSuffixGroup('test.xxx.yyy') == 'test.xxx'
        TrainSampleJob.keyPrefixOrSuffixGroup('test:xxx') == 'test:'
        TrainSampleJob.keyPrefixOrSuffixGroup('xxxyyyzzz') == 'xxxyyy'

        when:
        TrainSampleJob.keyPrefixOrSuffixGroupList = ['_zzz']
        then:
        TrainSampleJob.keyPrefixOrSuffixGroup('xxxyyy_zzz') == '_zzz'

        when:
        TrainSampleJob.keyPrefixOrSuffixGroupList = []
        then:
        TrainSampleJob.keyPrefixOrSuffixGroup('xxxyyyzzz') == 'xxxyyy'
    }

    def 'test train'() {
        given:
        TrainSampleJob.keyPrefixOrSuffixGroupList = []
        TrainSampleJob.setDictKeyPrefixEndIndex(5)

        TrainSampleJob.keyPrefixOrSuffixGroupList

        and:
        def job = new TrainSampleJob((byte) 0)
        job.dictSize = 512
        job.trainSampleMinBodyLength = 1024

        and:
        final String sampleValue = 'xxxx' * 5 + 'yyyy' * 5 + 'zzzz' * 5
        final byte[] sampleValueBytes = sampleValue.bytes

        final String longSampleValue = 'xxxx' * 200 + 'yyyy' * 200 + 'zzzz' * 200
        final byte[] longSampleValueBytes = longSampleValue.bytes

        def snowFlake = new SnowFlake(0, 0)

        TrainSampleJob.keyPrefixOrSuffixGroupList = ['key:']
        List<TrainSampleJob.TrainSampleKV> sampleToTrainList = []
        11.times {
            sampleToTrainList << new TrainSampleJob.TrainSampleKV("key:$it", null, snowFlake.nextId(), sampleValueBytes)
        }
        // will not be trained
        sampleToTrainList << new TrainSampleJob.TrainSampleKV("prefix:11", 'prefix:', snowFlake.nextId(), longSampleValueBytes)

        job.resetSampleToTrainList(sampleToTrainList)
        def result = job.train()

        expect:
        result.cacheDict().size() == 1
        result.removedSampleKVSeqList().size() == 11
        result.removedSampleKVSeqList() == sampleToTrainList.findAll { it.key().startsWith('key:') }.collect { it.seq() }

        when:
        def dict = result.cacheDict().get('key:')
        dict.initCtx()
        def cv = CompressedValue.compress(sampleValueBytes, dict)
        def decompressBytes = cv.decompress(dict)
        then:
        Arrays.equals(sampleValueBytes, decompressBytes)

        when:
        Debug.getInstance().logTrainDict = true
        sampleToTrainList.clear()
        10.times {
            sampleToTrainList << new TrainSampleJob.TrainSampleKV("key:$it", null, snowFlake.nextId(), sampleValueBytes)
        }
        job.resetSampleToTrainList(sampleToTrainList)
        // skip train, sample count not enough
        result = job.train()
        then:
        result == null

        when:
        Debug.getInstance().logTrainDict = true
        job.trainCount = 99
        // skip train, sample count not enough
        result = job.train()
        then:
        result == null

        when:
        TrainSampleJob.keyPrefixOrSuffixGroupList = ['prefix:']
        sampleToTrainList.clear()
        10.times {
            sampleToTrainList << new TrainSampleJob.TrainSampleKV("key:$it", null, snowFlake.nextId(), sampleValueBytes)
        }
        sampleToTrainList << new TrainSampleJob.TrainSampleKV("key:11", null, snowFlake.nextId(), longSampleValueBytes)
        job.resetSampleToTrainList(sampleToTrainList)
        // skip, dict already exists
        result = job.train()
        then:
        result.cacheDict().size() == 1

        when:
        job = new TrainSampleJob((byte) 0)
        job.dictSize = 512
        job.trainSampleMinBodyLength = 1024
        job.resetSampleToTrainList(sampleToTrainList)
        result = job.train()
        then:
        result.cacheDict().size() == 1
    }
}
