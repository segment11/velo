package ext.tools

import io.velo.repl.support.ExtendProtocolCommand
import redis.clients.jedis.DefaultJedisClientConfig
import redis.clients.jedis.Jedis

def host = System.getProperty('velo.host') ?: 'localhost'
int port = System.getProperty('velo.port')?.toInteger() ?: 7379

int slotNumber = 8

def config = DefaultJedisClientConfig.builder().timeoutMillis(1000 * 60 * 2).build()

for (i in 0..<slotNumber) {
    final int ii = i
    Thread.start {
        def jedis = new Jedis(host, port, config)
        def command = new ExtendProtocolCommand('manage')
        println 'Sending warm up command to slot ' + ii
        byte[] r = jedis.sendCommand(command, 'slot'.bytes, ii.toString().bytes, 'key-buckets-warm-up'.bytes) as byte[]
        println "Warm up for slot $ii: ${new String(r)}"
        jedis.close()
    }
}

