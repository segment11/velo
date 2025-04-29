package io.velo.tools

import redis.clients.jedis.Jedis
import redis.clients.jedis.params.ScanParams

def jedis = new Jedis('localhost', 7379)

def scanParams = new ScanParams().count(1000)
def result = jedis.scan('0', scanParams)

while (result.cursor != '0') {
    println 'cursor: ' + result.cursor + ', keys count: ' + result.result.size()
    result = jedis.scan(result.cursor, scanParams)
}

jedis.close()

