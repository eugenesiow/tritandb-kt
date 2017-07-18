package com.tritandb.engine.server

import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.util.DaemonThreadFactory
import com.natpryce.konfig.*
import com.tritandb.engine.query.op.RangeFlatChunk
import com.tritandb.engine.tsc.Compressor
import com.tritandb.engine.tsc.CompressorFlatChunk
import com.tritandb.engine.tsc.data.EventProtos
import com.tritandb.engine.tsc.data.buildTritanEvent
import org.zeromq.ZMQ
import org.zeromq.ZMsg
import java.io.File
import kotlin.system.measureTimeMillis
import com.sun.jmx.snmp.EnumRowStatus.destroy
import sun.net.sdp.SdpSupport.createSocket
import org.zeromq.ZContext



/**
 * TritanDb
 * Created by eugene on 18/07/2017.
 */
class ZmqReqRepServer(val config: Configuration) {

//    data class FileCompressor(val compressor: Compressor)
    data class DisruptorEvent(var value:ZMsg = ZMsg.newStringMsg()) {
        var arr:MutableList<Long> = mutableListOf()
        var timestamp:Long = 0
    }

    private object server : PropertyGroup() {
        val port by intType
        val host by stringType
        val bufferSize by intType
        val dataDir by stringType
    }

    //    private val C:MutableMap<String, FileCompressor> = mutableMapOf()
    private val C:MutableMap<String, Compressor> = mutableMapOf()

    private val handler: EventHandler<DisruptorEvent> = EventHandler({ (msg), _, _ ->
        val address = msg.pop()
        val content = msg.pop()
        msg.destroy()

        println(EventProtos.TritanEvent.parseFrom(address.data))
        println(content)

//        when(tEvent.type) {
//            EventProtos.TritanEvent.EventType.INSERT -> {
//                if(tEvent.hasRows()) {
//                    val firstRow = tEvent.rows.getRow(0)
//                    val c = GetCompressor(tEvent.name,firstRow.timestamp,firstRow.valueCount)
//                    for (row in tEvent.rows.rowList) {
//                        c.addRow(row.timestamp, row.valueList)
//                    }
//                }
//            }
//            EventProtos.TritanEvent.EventType.QUERY -> {
//                if(tEvent.hasRows()) {
//                    val firstRow = tEvent.rows.getRow(0)
//                    val bw = File("${config[server.dataDir]}/query_output.txt").bufferedWriter()
//                    println(measureTimeMillis { for(r in RangeFlatChunk("${config[server.dataDir]}/${tEvent.name}.tsc").run(firstRow.timestamp,firstRow.getValue(0))) {
//                        bw.append(r.timestamp.toString())
//                        for(value in r.values)
//                            bw.append(",$value")
//                        bw.newLine()
//                    } })
//                }
//            }
//            EventProtos.TritanEvent.EventType.INSERT_META -> {
//                println(tEvent.name)
//            }
//            EventProtos.TritanEvent.EventType.CLOSE -> {
//                val c = C.getValue(tEvent.name)
//                c.close()
//                C.remove(tEvent.name)
//                println("closed")
//            }
//            else -> {
//            }
//        }
    })

    private fun GetCompressor(name: String, timestamp: Long, valueCount: Int): Compressor {
        if(C.containsKey(name))
            return C.getValue(name)
        else {
//            val o:OutputStream = File("${config[server.dataDir]}/${name}.tsc").outputStream()
//            val b:BitOutput = BitByteBufferWriter(o)
//            val b:BitOutput = BitWriter(o)
//            val c: Compressor = CompressorFlat(timestamp,b,valueCount)
            val c: Compressor = CompressorFlatChunk("${config[server.dataDir]}/${name}.tsc",valueCount,4096 * 16)
            C.put(name,c)
            return c
        }
    }
//
//    private class server_task(val host:String,val port:Int) : Runnable {
//
//        override fun run() {
//            val ctx = ZContext()
//
//            //  Frontend socket talks to clients over TCP
//            val frontend = ctx.createSocket(ZMQ.ROUTER)
//            frontend.bind("$host:$port")
//
//            //  Backend socket talks to workers over inproc
//            val backend = ctx.createSocket(ZMQ.DEALER)
//            backend.bind("inproc://backend")
//
//            //  Connect backend to frontend via a proxy
//            ZMQ.proxy(frontend, backend, null)
//
//            ctx.destroy()
//        }
//    }


    fun start() {
//        Thread(server_task(config[server.host],config[server.port])).start()

        val bufferSize = config[server.bufferSize] // Specify the size of the ring buffer, must be power of 2.
        val disruptor = Disruptor(EventFactory ({ DisruptorEvent() }), bufferSize, DaemonThreadFactory.INSTANCE)

        disruptor.handleEventsWith(handler)
        disruptor.start() // Start the Disruptor, starts all threads running

        // Get the ring buffer from the Disruptor to be used for publishing.
        val ringBuffer = disruptor.ringBuffer

        val context = ZMQ.context(1)
        val receiver = context.socket(ZMQ.DEALER)
        //tcp://localhost:5700
//        receiver.connect("inproc://backend")
        receiver.connect("${config[server.host]}:${config[server.port]}")
        while (!Thread.currentThread().isInterrupted) {
            val msg = ZMsg.recvMsg(receiver)
            ringBuffer.publishEvent { event, _ -> event.value = msg}
        }
    }
}