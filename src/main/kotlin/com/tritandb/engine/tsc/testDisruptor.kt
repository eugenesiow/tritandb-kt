package com.tritandb.engine.tsc

import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.RingBuffer
import com.lmax.disruptor.dsl.Disruptor
import com.tritandb.engine.tsc.data.*
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType.CLOSE
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType.INSERT
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitOutput
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.OutputStream
import java.util.concurrent.Executors
import kotlin.system.measureTimeMillis


/**
* TritanDb
* Created by eugene on 12/05/2017.
*/

val o: OutputStream = File("shelburne.tsc").outputStream()
//val b: BitOutput = BitWriter(o)
val b: BitOutput = BitByteBufferWriter(o)
val c: CompressorFlat = CompressorFlat(1271692742104,b,6)
//var count = 0

val handler:EventHandler<DisruptorEvent> = EventHandler({ (tEvent), _, _ ->
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

fun main(args: Array<String>) {
    // Executor that will be used to construct new threads for consumers
    val executor = Executors.newSingleThreadExecutor()

    // Specify the size of the ring buffer, must be power of 2.
    val bufferSize = 1024

    // Construct the Disruptor
    @Suppress("DEPRECATION")
    val disruptor = Disruptor(EventFactory ({ DisruptorEvent() }), bufferSize, executor) //TODO: change to threadfactory

    // Connect the handler
    disruptor.handleEventsWith(handler)

    // Start the Disruptor, starts all threads running
    disruptor.start()

    // Get the ring buffer from the Disruptor to be used for publishing.
    val ringBuffer = disruptor.ringBuffer

    println("Time: ${measureTimeMillis{shelburne(ringBuffer)}}")
}

fun shelburne(ringBuffer: RingBuffer<DisruptorEvent>) {
    val br = BufferedReader(FileReader("/Users/eugene/Documents/Programming/data/shelburne/shelburne_test.csv"))
    br.readLine() //header
    for(line in br.lines()) {
        val parts = line.split(",")
        if(parts.size>6) {
            ringBuffer.publishEvent { event, _ ->
                event.value = buildTritanEvent {
                    type = INSERT
                    name = "shelburne"
                    rows = buildRows {
                        addRow(buildRow {
                            timestamp = (parts[0].toLong()/1000000)
                            for(i in 1..6) {
                                if(parts[i] == "")
                                    return@publishEvent
                                addValue(java.lang.Double.doubleToLongBits(parts[i].toDouble()))
                            }
                        })
                    }
                }
            }
        }
    }

    ringBuffer.publishEvent { event, _ -> event.value = buildTritanEvent {
        type = CLOSE
        name = "shelburne"
    } }
}
