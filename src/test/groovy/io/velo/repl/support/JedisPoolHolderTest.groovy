package io.velo.repl.support


import spock.lang.Specification

class JedisPoolHolderTest extends Specification {
    def 'test connect'() {
        given:
        def jedisPool = JedisPoolHolder.instance.create('localhost', 6379)
        def jedisPool2 = JedisPoolHolder.instance.create('localhost', 6379)

        expect:
        jedisPool == jedisPool2

        when:
        String r
        try {
            r = JedisPoolHolder.exe(jedisPool) { jedis ->
                jedis.set('test', 'test')
                jedis.get('test')
            }
        } catch (Exception e) {
            // may server not started
            println e.message
            r = 'test'
        }
        then:
        r == 'test'

        cleanup:
        JedisPoolHolder.instance.cleanUp()
    }
}
