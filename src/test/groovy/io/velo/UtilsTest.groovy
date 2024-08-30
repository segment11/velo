package io.velo


import spock.lang.Specification

class UtilsTest extends Specification {
    def 'test base'() {
        given:
        println Utils.projectPath('')

        expect:
        Utils.leftPad("abc", 'x', 5) == "xxabc"
        Utils.leftPad("abcde", 'x', 5) == "abcde"

        Utils.rightPad("abc", 'x', 5) == "abcxx"
        Utils.rightPad("abcde", 'x', 5) == "abcde"

        new File(Utils.projectPath('/src/main/java/io/velo/Utils.java')).exists()
    }
}
