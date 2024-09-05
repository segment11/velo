package io.velo.repl.cluster

import io.velo.persist.Consts
import spock.lang.Specification

class ShardTest extends Specification {
    def 'test all'() {
        given:
        Consts.persistDir.mkdirs()
        def shard = new Shard(Consts.persistDir)

        when:
        shard.saveMeta()
        shard.loadMeta()
        then:
        shard.master() == null
        shard.slave(0) == null

        cleanup:
        Consts.persistDir.deleteDir()
    }
}
