package io.velo.command

import com.moilioncircle.redis.replicator.RedisReplicator
import com.moilioncircle.redis.replicator.rdb.datatype.AuxField
import com.moilioncircle.redis.replicator.rdb.datatype.ExpiredType
import com.moilioncircle.redis.replicator.rdb.datatype.KeyValuePair
import com.moilioncircle.redis.replicator.rdb.datatype.ZSetEntry
import com.moilioncircle.redis.replicator.rdb.iterable.ValueIterableRdbVisitor

def r = new RedisReplicator('redis:///home/kerry/ws/test/redis6/dump.rdb')
r.rdbVisitor = new ValueIterableRdbVisitor(r)

r.addEventListener { rep, event ->
    if (event instanceof KeyValuePair) {
        def kv = (KeyValuePair) event
        print new String(kv.key)

        if (kv.expiredType != ExpiredType.NONE) {
            print kv.expiredType.name() + '->' + kv.expiredValue + ': '
        }

        if (kv.value instanceof Iterator) {
            print ' iterator ' + kv.valueRdbType + ': '

            def it = (Iterator) kv.value
            while (it.hasNext()) {
                def item = it.next()
                if (item instanceof Map.Entry) {
                    def entry = (Map.Entry) item
                    print new String(entry.getKey()) + '->' + new String(entry.getValue())
                } else if (item instanceof ZSetEntry) {
                    def entry = (ZSetEntry) item
                    print new String(entry.getElement()) + '->' + entry.getScore()
                } else if (item instanceof byte[]) {
                    print new String((byte[]) item)
                }
            }
            println ''
        } else if (kv.value instanceof byte[]) {
            print ' bytes ' + kv.valueRdbType + ': '
            println new String(kv.value)
        } else {
            println 'value class: ' + kv.value.getClass()
        }
    } else if (event instanceof AuxField) {
        def aux = (AuxField) event
        println 'aux field: ' + aux.auxKey + '->' + aux.auxValue
    } else {
        println 'event class: ' + event.getClass()
        return
    }
}

r.open()