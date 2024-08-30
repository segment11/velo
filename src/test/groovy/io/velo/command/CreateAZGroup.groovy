package io.velo.command

def getTpl = { String c ->
    def upper = c.toUpperCase()
    """
package io.velo.command;

import io.velo.reply.NilReply;
import io.velo.reply.Reply;

public class ${upper}Group extends Base {
    public ${upper}Group(String cmd, byte[][] data, ITcpSocket socket) {
        super(cmd, data, socket);
    }

    public Reply handle() {
        return NilReply.INSTANCE;
    }
}
"""
}

def projectRoot = '../../../../'
def dir = new File(projectRoot + "main/java/io/velo/command")
println dir.absolutePath
println dir.exists()

//('a'..'z').each {
//    def file = new File(dir, "${it.toUpperCase()}Group.java")
//    // skip
//    if (it == 'r') {
//        return
//    }
////    if (!file.exists()) {
//    file.text = getTpl(it)
//    println "create ${file.absolutePath}"
////    }
//}

('a'..'z').each {
//    println """
//else if (firstByte == '${it}' || firstByte == '${it.toUpperCase()}') {
//            return new ${it.toUpperCase()}Group(cmd, data, socket).handle();
//        }
//"""

    println """
else if (firstByte == '${it}' || firstByte == '${it.toUpperCase()}') {
            slot = ${it.toUpperCase()}Group.parseSlot(data);
        }
"""
}


