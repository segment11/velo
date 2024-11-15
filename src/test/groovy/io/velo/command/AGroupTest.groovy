package io.velo.command


import io.velo.AfterAuthFlagHolder
import io.velo.BaseCommand
import io.velo.SocketInspectorTest
import io.velo.ValkeyRawConfSupport
import io.velo.acl.AclUsers
import io.velo.acl.RCmd
import io.velo.acl.U
import io.velo.mock.InMemoryGetSet
import io.velo.reply.*
import spock.lang.Specification

import java.nio.file.Paths

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
        ((BulkReply) ((MultiBulkReply) reply).replies[0]).raw == 'all'.bytes

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
        reply == ErrorReply.ACL_PERMIT_LIMIT

        when:
        AclUsers.instance.upInsert('a') {
            it.on = false
            it.password = U.Password.NO_PASSWORD
        }
        reply = aGroup.execute('acl dryrun a get a')
        then:
        reply == ErrorReply.ACL_PERMIT_LIMIT

        when:
        AclUsers.instance.upInsert('a') {
            it.on = true
        }
        reply = aGroup.execute('acl dryrun a get a')
        then:
        reply == ErrorReply.ACL_PERMIT_LIMIT

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
        reply = aGroup.execute('acl getuser a')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = aGroup.execute('acl getuser b')
        then:
        reply == NilReply.INSTANCE

        when:
        reply = aGroup.execute('acl getuser')
        then:
        reply == ErrorReply.SYNTAX

        when:
        reply = aGroup.execute('acl list')
        then:
        reply instanceof MultiBulkReply
        // default + a
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = aGroup.execute('acl list a')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        var aclFile = Paths.get(ValkeyRawConfSupport.aclFilename).toFile();
        if (!aclFile.exists()) {
            aclFile.createNewFile()
        }
        aclFile.text = 'user default on nopass ~* +@all &*\r\n# comment'
        reply = aGroup.execute('acl load')
        then:
        reply == OKReply.INSTANCE

        when:
        aclFile.text = 'user a on nopass ~* +@all &*'
        reply = aGroup.execute('acl load')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message == 'no default user in acl file'

        when:
        aclFile.text = 'user default on'
        reply = aGroup.execute('acl load')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message.contains 'parse acl file error'

        when:
        // invalid key pattern
        aclFile.text = 'user default on nopass %~* +@all &*'
        reply = aGroup.execute('acl load')
        then:
        reply instanceof ErrorReply
        ((ErrorReply) reply).message.contains 'parse acl file error'

        when:
        aclFile.delete()
        reply = aGroup.execute('acl load')
        then:
        reply == ErrorReply.NO_SUCH_FILE

        when:
        reply = aGroup.execute('acl load x')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        reply = aGroup.execute('acl log reset')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = aGroup.execute('acl log 1')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 1

        when:
        reply = aGroup.execute('acl log 0')
        then:
        reply instanceof ErrorReply

        when:
        reply = aGroup.execute('acl log 101')
        then:
        reply instanceof ErrorReply

        when:
        reply = aGroup.execute('acl log a')
        then:
        reply == ErrorReply.INVALID_INTEGER

        when:
        reply = aGroup.execute('acl log')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        aclFile.delete()
        reply = aGroup.execute('acl save')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = aGroup.execute('acl save x')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        reply = aGroup.execute('acl setuser a reset +@all ~* &*')
        then:
        reply == OKReply.INSTANCE

        when:
        reply = aGroup.execute('acl setuser a reset')
        then:
        reply == OKReply.INSTANCE
        !AclUsers.instance.get('a').on

        when:
        reply = aGroup.execute('acl setuser a')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        reply = aGroup.execute('acl users')
        then:
        reply instanceof MultiBulkReply
        ((MultiBulkReply) reply).replies.length == 2

        when:
        reply = aGroup.execute('acl users a')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        def socket = SocketInspectorTest.mockTcpSocket()
        aGroup.socket = socket
        reply = aGroup.execute('acl whoami')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'default'.bytes

        when:
        AfterAuthFlagHolder.add(socket.remoteAddress, 'a')
        reply = aGroup.execute('acl whoami')
        then:
        reply instanceof BulkReply
        ((BulkReply) reply).raw == 'a'.bytes

        when:
        reply = aGroup.execute('acl whoami x')
        then:
        reply == ErrorReply.SYNTAX

        // ***** *****
        when:
        reply = aGroup.execute('acl')
        then:
        reply == ErrorReply.FORMAT
    }
}
