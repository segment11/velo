package io.velo.tool.pcap.replay

import groovy.transform.CompileStatic
import groovyjarjarpicocli.CommandLine
import io.netty.buffer.ByteBuf
import io.netty.buffer.Unpooled
import io.velo.tool.pcap.RESP
import io.velo.tool.pcap.extend.TablePrinter
import io.vproxy.base.util.ByteArray
import io.vproxy.base.util.bytearray.SubByteArray
import io.vproxy.vpacket.EthernetPacket
import io.vproxy.vpacket.Ipv4Packet
import io.vproxy.vpacket.PacketDataBuffer
import io.vproxy.vpacket.TcpPacket
import org.pcap4j.core.BpfProgram
import org.pcap4j.core.PcapHandle
import org.pcap4j.core.PcapNetworkInterface
import org.pcap4j.core.Pcaps
import org.pcap4j.packet.Packet
import org.pcap4j.packet.UnknownPacket

import java.sql.Timestamp
import java.util.concurrent.Callable

@CompileStatic
@CommandLine.Command(name = 'java -jar pcap_resp_replay-1.0.0.jar', mixinStandardHelpOptions = true, version = 'io.velo pcap_resp_replay 1.0.0',
        description = 'TCP monitor / filter and then replay / redirect to target redis server. Need root permission.')
class MonitorAndReplay implements Callable<Integer> {
    @CommandLine.Option(names = ['-i', '--interface'], description = 'interface, eg: eth0')
    String itf = 'enp4s0'

    @CommandLine.Option(names = ['-p', '--port'], description = 'port, eg: 6379')
    int port = 6379

    @CommandLine.Option(names = ['-h', '--target-host'], description = 'target host, eg: localhost')
    String targetHost = 'localhost'

    @CommandLine.Option(names = ['-t', '--target-port'], description = 'target port, eg: 6380')
    int targetPort = 6380

    @CommandLine.Option(names = ['-r', '--read-timeout'], description = 'read timeout seconds, default: 10')
    int readTimeout = 10

    @CommandLine.Option(names = ['-b', '--buffer-size'], description = 'buffer size, default: 65536')
    int bufferSize = 65536

    @CommandLine.Option(names = ['-f', '--filter'], description = 'filter, default: tcp dst port 6379')
    String filter = 'tcp dst port 6379'

    @CommandLine.Option(names = ['-c', '--max-packet-count'], description = 'max packet count, default: 100')
    int maxPacketCount = 100

    private final RESP resp = new RESP()

    private ByteBuf buf

    private void handlePacket(Packet packet, Timestamp timestamp) {
        def p = (UnknownPacket) packet

        def byteArray = ByteArray.from(p.rawData)
        def ethPacket = new EthernetPacket()
        ethPacket.from(new PacketDataBuffer(byteArray))

        def tcpPacket = ((Ipv4Packet) ethPacket.packet).packet as TcpPacket
        def byteArray2 = tcpPacket.data
        if (byteArray2.length() == 0) {
            // skip empty packet
            return
        }

        if (!(byteArray2 instanceof SubByteArray)) {
            return
        }

        def subByteArray = (SubByteArray) byteArray2
        def rawBytes = subByteArray.toJavaArray()

        buf.readerIndex(0)
        buf.writerIndex(0)
        buf.writeBytes(rawBytes)

        def data = resp.decode(buf)
        println 'data length: ' + data.length
        println 'timestamp: ' + timestamp

        // write to redis server, use jedis, todo
    }

    @Override
    Integer call() throws Exception {
        println 'interface: ' + itf
        println 'port: ' + port
        println 'targetHost: ' + targetHost
        println 'targetPort: ' + targetPort
        println 'readTimeout: ' + readTimeout
        println 'bufferSize: ' + bufferSize
        println 'filter: ' + filter
        println 'maxPacketCount: ' + maxPacketCount

        buf = Unpooled.buffer(bufferSize)

        def nif = Pcaps.getDevByName(itf)
        if (nif == null) {
            println 'No such device: ' + itf
            return 1
        }
        println 'get nif: ' + nif.description

        def phb = new PcapHandle.Builder(nif.getName())
                .promiscuousMode(PcapNetworkInterface.PromiscuousMode.PROMISCUOUS)
                .timeoutMillis(readTimeout * 1000)
                .bufferSize(bufferSize)
                .timestampPrecision(PcapHandle.TimestampPrecision.MICRO)

        def handle = phb.build()
        handle.setFilter(filter, BpfProgram.BpfCompileMode.OPTIMIZE)

        int num = 0
        while (true) {
            def packet = handle.nextPacket
            if (packet != null) {
                handlePacket(packet, handle.timestamp)
                num++
                if (num >= maxPacketCount) {
                    break
                }
            }
        }

        def ps = handle.stats

        List<List> table = []
        table << ['ps_recv', 'ps_drop', 'ps_ifdrop']
        table << [ps.numPacketsReceived, ps.numPacketsDropped, ps.numPacketsDroppedByIf]
        TablePrinter.print(table)

        handle.close()

        0
    }

    static void main(String[] args) {
        int exitCode = new CommandLine(new MonitorAndReplay()).execute(args)
        System.exit(exitCode)
    }
}