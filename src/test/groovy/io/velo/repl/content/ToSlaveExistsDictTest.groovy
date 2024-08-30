package io.velo.repl.content

import io.activej.bytebuf.ByteBuf
import io.velo.Dict
import spock.lang.Specification

import java.nio.ByteBuffer

class ToSlaveExistsDictTest extends Specification {
    def 'test all'() {
        given:
        HashMap<String, Dict> cacheDict = [:]
        TreeMap<Integer, Dict> cacheDictBySeq = new TreeMap<>()
        ArrayList<Integer> sentDictSeqList = []
        def content = new ToSlaveExistsDict(cacheDict, cacheDictBySeq, sentDictSeqList)

        expect:
        content.encodeLength() == 4

        when:
        def bytes = new byte[content.encodeLength()]
        def buf = ByteBuf.wrapForWriting(bytes)
        content.encodeTo(buf)
        def buffer = ByteBuffer.wrap(bytes)
        then:
        buffer.getInt() == 0

        when:
        def dict1 = new Dict(new byte[10])
        def dict2 = new Dict(new byte[20])
        def dict3 = new Dict(new byte[30])
        def dict4 = new Dict(new byte[40])
        cacheDict.put("1", dict1)
        cacheDict.put("2", dict2)
        cacheDict.put("3", dict3)
        cacheDict.put("4", dict4)
        cacheDictBySeq.put(1, dict1)
        cacheDictBySeq.put(2, dict2)
        cacheDictBySeq.put(3, dict3)
        cacheDictBySeq.put(4, dict4)
        sentDictSeqList << 1
        sentDictSeqList << 2
        def content2 = new ToSlaveExistsDict(cacheDict, cacheDictBySeq, sentDictSeqList)
        def bytes2 = new byte[content2.encodeLength()]
        def buf2 = ByteBuf.wrapForWriting(bytes2)
        content2.encodeTo(buf2)
        def buffer2 = ByteBuffer.wrap(bytes2)
        then:
        buffer2.getInt() == 2
    }
}
