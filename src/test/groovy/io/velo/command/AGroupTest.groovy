package io.velo.command

import io.velo.BaseCommand
import io.velo.acl.AclUsers
import io.velo.acl.RCmd
import io.velo.acl.U
import io.velo.mock.InMemoryGetSet
import io.velo.reply.*
import spock.lang.Specification

class AGroupTest extends Specification {
    def _AGroup = new AGroup(null, null, null)

    def 'test parse slot'() {
        given:
        def data3 = new byte[3][]
        int slotNumber = 128

        and:
        data3[1] = 'a'.bytes

        when:
        def sAppendList = _AGroup.parseSlots('append', data3, slotNumber)
        then:
        sAppendList.size() == 1

        when:
        sAppendList = _AGroup.parseSlots('axxx', data3, slotNumber)
        then:
        sAppendList.size() == 0

        when:
        def data1 = new byte[1][]
        sAppendList = _AGroup.parseSlots('append', data1, slotNumber)
        then:
        sAppendList.size() == 0
    }

    def 'test handle'() {
        given:
        def data1 = new byte[1][]

        def aGroup = new AGroup('append', data1, null)
        aGroup.from(BaseCommand.mockAGroup())

        when:
        def reply = aGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        aGroup.cmd = 'acl'
        reply = aGroup.handle()
        then:
        reply == ErrorReply.FORMAT

        when:
        aGroup.cmd = 'zzz'
        reply = aGroup.handle()
        then:
        reply == NilReply.INSTANCE
    }

    def 'test append'() {
        def inMemoryGetSet = new InMemoryGetSet()

        def aGroup = new AGroup(null, null, null)
        aGroup.byPassGetSet = inMemoryGetSet
        aGroup.from(BaseCommand.mockAGroup())

        when:
        aGroup.execute('append a 123')
        then:
        aGroup.get('a'.bytes, aGroup.slot('a'.bytes)) == '123'.bytes

        when:
        aGroup.execute('append a 456')
        then:
        aGroup.get('a'.bytes, aGroup.slot('a'.bytes)) == '123456'.bytes

        when:
        def reply = aGroup.execute('append a')
        then:
        reply == ErrorReply.FORMAT
    }

    def 'test acl'() {
        given:
        def inMemoryGetSet = new InMemoryGetSet()

        def aGroup = new AGroup(null, null, null)
        aGroup.byPassGetSet = inMemoryGetSet
        aGroup.from(BaseCommand.mockAGroup())

        and:
        AclUsers.instance.initForTest()

        // ***** *****
        when:
        def reply = aGroup.execute('acl cat')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'admin'.bytes

        when:
        reply = aGroup.execute('acl cat dangerous')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies[0] instanceof BulkReply
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'acl'.bytes

        when:
        reply = aGroup.execute('acl cat dangerous_x')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = aGroup.execute('acl cat a b')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = aGroup.execute('acl cat_x')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        reply = aGroup.execute('acl deluser')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = aGroup.execute('acl deluser a')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 0

        when:
        AclUsers.instance.upInsert('a') {
            it.password = U.Password.NO_PASSWORD
        }
        reply = aGroup.execute('acl deluser a')
        then:
        reply instanceof IntegerReply
        ((IntegerReply) reply).integer == 1

        // ***** *****
        when:
        reply = aGroup.execute('acl dryrun a get a')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message == 'no such user'

        when:
        AclUsers.instance.upInsert('a') {
            it.on = false
            it.password = U.Password.NO_PASSWORD
        }
        reply = aGroup.execute('acl dryrun a get a')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message == 'user is disabled'

        when:
        AclUsers.instance.upInsert('a') {
            it.on = true
        }
        reply = aGroup.execute('acl dryrun a get a')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message == ErrorReply.ACL_PERMIT_LIMIT.message

        when:
        AclUsers.instance.upInsert('a') {
            it.addRCmd(true, RCmd.fromLiteral('+get'))
        }
        reply = aGroup.execute('acl dryrun a get a')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = aGroup.execute('acl dryrun a')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        reply = aGroup.execute('acl genpass')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 64

        when:
        reply = aGroup.execute('acl genpass 5')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw.length == 2

        when:
        reply = aGroup.execute('acl genpass 0')
        then:
        reply instanceof ErrorReply

        when:
        reply = aGroup.execute('acl genpass 1025')
        then:
        reply instanceof ErrorReply

        when:
        reply = aGroup.execute('acl genpass x')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = aGroup.execute('acl genpass 1 2')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        reply = aGroup.execute('acl')
        then:
        reply == ErrorReply.FORMAT
    }
}
