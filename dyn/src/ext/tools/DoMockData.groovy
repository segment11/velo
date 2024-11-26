package ext.tools

import io.velo.repl.support.ExtendProtocolCommand
import redis.clients.jedis.DefaultJedisClientConfig
import redis.clients.jedis.Jedis

def host = System.getProperty('velo.host') ?: 'localhost'
int port = System.getProperty('velo.port')?.toInteger() ?: 7379

int slotNumber = 8
// mock 8000w data cost may be 2 minute
int n = 80_000_000
int valueLength = 200

def config = DefaultJedisClientConfig.builder().timeoutMillis(1000 * 60 * 2).build()

for (i in 0..<slotNumber) {
    final int ii = i
    Thread.start {
        def jedis = new Jedis(host, port, config)
        def command = new ExtendProtocolCommand('manage')
        println 'Sending mock data to slot ' + ii
        byte[] r = jedis.sendCommand(command, 'slot'.bytes, ii.toString().bytes, 'mock-data'.bytes,
                "nn=${n}".toString().bytes, "k=16".bytes, "d=${valueLength}".toString().bytes) as byte[]
        println "Mock data for slot $ii: ${new String(r)}"
        jedis.close()
    }
}

