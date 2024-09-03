package io.velo.persist.index

import io.velo.ConfForGlobal
import io.velo.persist.Consts
import spock.lang.Specification

class MetaIndexWordsTest extends Specification {
    final byte workerId = 0

    def 'test all'() {
        given:
        def metaIndexWords = new MetaIndexWords(workerId, Consts.indexDir)

        when:
        metaIndexWords.clear()
        then:
        1 == 1

        when:
        metaIndexWords.cleanUp()
        def metaIndexWords2 = new MetaIndexWords(workerId, Consts.indexDir)
        then:
        1 == 1

        when:
        metaIndexWords2.cleanUp()
        ConfForGlobal.pureMemory = true
        def metaIndexWords3 = new MetaIndexWords(workerId, Consts.indexDir)
        metaIndexWords3.clear()
        then:
        1 == 1

        when:
        // eg length <= 8
        def bad = 'bad'
        def badUpperCase = 'BAD'
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(bad.bytes[0], bad.bytes[1], (byte) bad.length())
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(badUpperCase.bytes[0], badUpperCase.bytes[1], (byte) badUpperCase.length())
        // eg length <= 16
        def calculator = 'calculator'
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(calculator.bytes[0], calculator.bytes[1], (byte) calculator.length())
        // eg length <= 32
        def unenthusiastically = 'unenthusiastically'
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(unenthusiastically.bytes[0], unenthusiastically.bytes[1], (byte) unenthusiastically.length())
        then:
        1 == 1

        when:
        // not alphabet, length <= 8
        def notAlphabet0 = '123'
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(notAlphabet0.bytes[0], notAlphabet0.bytes[1], (byte) notAlphabet0.length())
        // not alphabet, length <= 16
        def notAlphabet1 = '1234567890123456'
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(notAlphabet1.bytes[0], notAlphabet1.bytes[1], (byte) notAlphabet1.length())
        // not alphabet, length <= 32
        def notAlphabet2 = '12345678901234567890'
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(notAlphabet2.bytes[0], notAlphabet2.bytes[1], (byte) notAlphabet2.length())
        then:
        1 == 1

        when:
        // not all alphabet, length <= 8
        def notAllAlphabet0 = '1a'
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(notAllAlphabet0.bytes[0], notAllAlphabet0.bytes[1], (byte) notAllAlphabet0.length())
        def notAllAlphabet1 = 'a1'
        metaIndexWords3.wordGroupOffsetAfterHeaderForMeta(notAllAlphabet1.bytes[0], notAllAlphabet1.bytes[1], (byte) notAllAlphabet1.length())
        then:
        1 == 1
        metaIndexWords3.wordLength8or16or32((byte) 8) == 8
        metaIndexWords3.wordLength8or16or32((byte) 16) == 16
        metaIndexWords3.wordLength8or16or32((byte) 32) == 32

        when:
        metaIndexWords3.clear()
        metaIndexWords3.cleanUp()
        ConfForGlobal.pureMemory = false
        def metaIndexWords4 = new MetaIndexWords(workerId, Consts.indexDir)
        TreeSet<String> wordSet = []
        wordSet << 'bad'
        wordSet << 'calculator'
        wordSet << 'calculate'
        wordSet << 'ca_123456'
        wordSet << 'unenthusiastically'
        metaIndexWords4.putWords(wordSet)
        then:
        metaIndexWords4.afterPutWordCount() == 5
        metaIndexWords4.getOneWordMeta('bad') != null
        metaIndexWords4.getOneWordMeta('calculator') != null
        metaIndexWords4.getOneWordMeta('calculate') != null
        metaIndexWords4.getOneWordMeta('ca_123456') != null
        metaIndexWords4.getOneWordMeta('unenthusiastically') != null
        metaIndexWords4.getOneWordMeta('unenthusiastically_') == null

        when:
        metaIndexWords4.cleanUp()
        def metaIndexWords5 = new MetaIndexWords(workerId, Consts.indexDir)
        println metaIndexWords5
        then:
        metaIndexWords5.afterPutWordCount() == 5

        when:
        // already exist
        metaIndexWords5.putWord('bad', 0, 0)
        then:
        metaIndexWords5.afterPutWordCount() == 5

        when:
        metaIndexWords5.putWord('cake', 0, 0)
        then:
        metaIndexWords5.afterPutWordCount() == 6

        when:
        metaIndexWords5.putWord('cake', 0, 1)
        then:
        // read from memory bytes
        metaIndexWords5.getOneWordMeta('cake').totalCount() == 1
        // read from hash map
        metaIndexWords5.getTotalCount('cake') == 1

        when:
        def exception = false
        try {
            metaIndexWords5.putWord('cake', 1, 0)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        // segment index not match
        exception

        when:
        metaIndexWords5.cleanUp()
        // load again
        def metaIndexWords6 = new MetaIndexWords(workerId, Consts.indexDir)
        then:
        metaIndexWords6.getTotalCount('cake') == 1

        when:
        metaIndexWords6.cleanUp()
        ConfForGlobal.pureMemory = true
        def metaIndexWords7 = new MetaIndexWords(workerId, Consts.indexDir)
        wordSet.clear()
        64.times {
            wordSet << 'key:' + it
        }
        metaIndexWords7.putWords(wordSet)
        then:
        metaIndexWords7.afterPutWordCount() == 64

        when:
        // pure memory mode update meta total count
        metaIndexWords7.putWord('key:63', 0, 1)
        then:
        metaIndexWords7.getTotalCount('key:63') == 1

        when:
        exception = false
        wordSet.clear()
        wordSet << 'key:64'
        try {
            metaIndexWords7.putWords(wordSet)
        } catch (IllegalStateException e) {
            println e.message
            exception = true
        }
        then:
        exception

        cleanup:
        metaIndexWords7.cleanUp()
        Consts.indexDir.deleteDir()
        ConfForGlobal.pureMemory = false
    }
}
