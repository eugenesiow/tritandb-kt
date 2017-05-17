package com.tritandb.engine.server

import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.util.DaemonThreadFactory
import com.natpryce.konfig.*
import com.tritandb.engine.tsc.CompressorFlat
import com.tritandb.engine.tsc.data.DisruptorEvent
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType.*
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitOutput
import org.zeromq.ZMQ
import java.io.File
import java.io.OutputStream


/**
 * TritanDb
 * Created by eugene on 17/05/2017.
 */
class ZmqServer(val config:Configuration) {
    init {

    }

    object server : PropertyGroup() {
        val port by intType
        val host by stringType
        val bufferSize by intType
    }

    val o: OutputStream = File("shelburne.tsc").outputStream()
    //val b: BitOutput = BitWriter(o)
    val b: BitOutput = BitByteBufferWriter(o)
    val c: CompressorFlat = CompressorFlat(1271692742104,b,6)

    val handler: EventHandler<DisruptorEvent> = EventHandler({ (tEvent), _, _ ->
        when(tEvent.type) {
            INSERT -> {
                for (row in tEvent.rows.rowList) {
                    c.addRow(row.timestamp, row.valueList)
                }
            }
            CLOSE -> {
                c.close()
                println("closed")
            }
            else -> {
            }
        }
    })


    fun start() {
        val bufferSize = config[server.bufferSize] // Specify the size of the ring buffer, must be power of 2.
        val disruptor = Disruptor(EventFactory ({ DisruptorEvent() }), bufferSize, DaemonThreadFactory.INSTANCE)

        disruptor.handleEventsWith(handler)
        disruptor.start() // Start the Disruptor, starts all threads running

        // Get the ring buffer from the Disruptor to be used for publishing.
        val ringBuffer = disruptor.ringBuffer

        val context = ZMQ.context(1)
        val receiver = context.socket(ZMQ.PULL)
        //tcp://localhost:5700
        receiver.bind("${config[server.host]}:${config[server.port]}")
        while (!Thread.currentThread().isInterrupted) {
            val msg = receiver.recv()
            ringBuffer.publishEvent { event, _ -> event.value = TritanEvent.parseFrom(msg)}
        }
    }
}