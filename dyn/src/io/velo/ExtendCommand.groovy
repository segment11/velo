package io.velo

import groovy.transform.CompileStatic
import io.activej.net.socket.tcp.ITcpSocket
import io.velo.reply.NilReply
import io.velo.reply.Reply

@CompileStatic
class ExtendCommand extends BaseCommand {
    static final String version = '1.0.0'

    ExtendCommand(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket)
    }

    @Override
    Reply handle() {
        log.info 'Dyn extend command version: {}', version
        return NilReply.INSTANCE
    }
}