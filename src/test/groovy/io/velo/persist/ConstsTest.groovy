package io.velo.persist

import spock.lang.Specification

class ConstsTest extends Specification {
    def 'test zookeeper helper constants'() {
        expect:
        Consts.zookeeperVersion == '3.8.6'
        Consts.zookeeperArchiveFile.absolutePath == '/tmp/apache-zookeeper-3.8.6-bin.tar.gz'
        Consts.zookeeperDownloadUrl() == 'https://dlcdn.apache.org/zookeeper/zookeeper-3.8.6/apache-zookeeper-3.8.6-bin.tar.gz'
        Consts.zookeeperWorkDir.absolutePath == '/tmp/velo-zookeeper'
    }

    def 'test download and start zookeeper'() {
        Consts.downloadAndStartZookeeper()
        expect:
        Consts.zookeeperWorkDir.exists()
        Consts.checkConnectAvailable()
    }
}
