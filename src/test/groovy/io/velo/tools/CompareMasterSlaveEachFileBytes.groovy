package io.velo.tools

import java.nio.ByteBuffer

def masterDir = new File('/tmp/velo-data2')

def compareOneFdBytes = { File masterFile, File slaveFile ->
    println 'ready to compare: ' + masterFile

    if (masterFile.name == 'meta_chunk_segment_index.dat') {
        // only compare first 4 bytes, chunk current segment index
        if (masterFile.bytes[0..3] != slaveFile.bytes[0..3]) {
            println "file bytes not equals, master file: $masterFile, chunk current segment index not equal"
            return false
        } else {
            println 'chunk current segment index equals: ' + masterFile
            return true
        }
    }

    // once compare 1M bytes
    def masterBuffer = new byte[1024 * 1024]
    def slaveBuffer = new byte[1024 * 1024]

    def masterStream = new FileInputStream(masterFile)
    def slaveStream = new FileInputStream(slaveFile)
    def masterRead = 0
    def slaveRead = 0
    def offset = 0
    while (masterRead != -1 && slaveRead != -1) {
        masterRead = masterStream.read(masterBuffer)
        slaveRead = slaveStream.read(slaveBuffer)
        if (masterRead > slaveRead) {
            println "file bytes not equals, master file: $masterDir, offset: $offset, master read: $masterRead, slave read: $slaveRead"
            return false
        }

        if (masterRead == -1) {
            break
        }

        if (ByteBuffer.wrap(masterBuffer, 0, masterRead) != ByteBuffer.wrap(slaveBuffer, 0, masterRead)) {
            println "file bytes not equals, master file: $masterFile, offset: $offset, master / slave buffer not equal"
            return false
        }
        offset += masterRead
    }

    println 'file bytes equals: ' + masterFile
    return true
}

boolean isAnyFileBytesNotEquals = false
masterDir.eachFileRecurse { masterFile ->
    if (masterFile.isDirectory()) {
        return
    }

    // skip dyn config file
    if ('dyn-config.json' == masterFile.name) {
        return
    }

    // skip binlog file
    if (masterFile.name.startsWith('binlog-')) {
        return
    }

    if (isAnyFileBytesNotEquals) {
        return
    }

    def slaveFile = new File(masterFile.absolutePath.replace('velo-data2', 'velo-data'))
    if (slaveFile.exists()) {
        def isFileBytesEquals = compareOneFdBytes(masterFile, slaveFile)
        if (!isFileBytesEquals) {
            println "file bytes not equals: $masterFile"
            isAnyFileBytesNotEquals = true
        }
    } else {
        println "slave file not exists: $slaveFile"
        isAnyFileBytesNotEquals = true
    }
}