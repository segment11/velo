package io.velo.decode

import io.velo.BaseCommand
import io.velo.acl.AclUsers
import io.velo.acl.RCmd
import io.velo.acl.U
import spock.lang.Specification

class RequestTest extends Specification {
    def 'test all'() {
        given:
        def data3 = new byte[3][]
        data3[0] = 'SET'.bytes
        data3[1] = 'key'.bytes
        data3[2] = 'value'.bytes

        def request = new Request(data3, false, false)
        request.slotNumber = 1
        request.u = U.INIT_DEFAULT_U

        request.bigStringNoMemoryCopy == null

        expect:
        request.data.length == 3
        !request.isHttp()
        !request.isRepl()
        request.u == U.INIT_DEFAULT_U

        request.cmd() == 'set'
        request.cmd() == 'set'
        request.singleSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER
        request.slotNumber == 1

        println request.toString()

        when:
        request.slotWithKeyHashList = [BaseCommand.slot('key'.bytes, 1)]
        then:
        request.slotWithKeyHashList.size() == 1
        request.singleSlot == request.slotWithKeyHashList[0].slot()

        when:
        request.slotWithKeyHashList = []
        then:
        request.singleSlot == Request.SLOT_CAN_HANDLE_BY_ANY_WORKER

        when:
        AclUsers.instance.initForTest()
        AclUsers.instance.upInsert('default') { u ->
            u.addRCmd(true, RCmd.fromLiteral('+*'))
        }
        then:
        request.isAclCheckOk()

        when:
        request.u = null
        then:
        request.isAclCheckOk()

        when:
        def dataRepl = new byte[3][]
        dataRepl[0] = new byte[8]
        dataRepl[1] = new byte[2]
        dataRepl[1][0] = (byte) 0
        def requestRepl = new Request(dataRepl, false, true)
        then:
        requestRepl.isRepl()
        requestRepl.singleSlot == 0

        when:
        data3[0] = 'MGET'.bytes
        data3[1] = 'key1'.bytes
        data3[2] = 'key2'.bytes
        def request2 = new Request(data3, true, false)
        request2.slotNumber = 2
        request2.slotWithKeyHashList = [
                BaseCommand.slot('key1'.bytes, 2),
                BaseCommand.slot('key2'.bytes, 2)
        ]
        def isMultiSlot = request2.slotWithKeyHashList.collect { it.slot() }.unique().size() > 1
        request2.setCrossRequestWorker(isMultiSlot)
        then:
        request2.isCrossRequestWorker() == isMultiSlot
        request2.singleSlot == request2.slotWithKeyHashList[0].slot()

        when:
        def data1 = new byte[1][]
        def httpRequest = new Request(data1, true, false)
        then:
        httpRequest.isHttp()
        httpRequest.getHttpHeader('key') == null

        when:
        httpRequest.httpHeaders = [:]
        then:
        httpRequest.getHttpHeader('key') == null

        when:
        httpRequest.httpHeaders = ['key': 'value']
        then:
        httpRequest.getHttpHeader('key') == 'value'
    }
}
