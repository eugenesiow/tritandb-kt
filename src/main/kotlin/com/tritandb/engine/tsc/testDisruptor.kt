package com.tritandb.engine.tsc

import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.dsl.Disruptor
import com.tritandb.engine.tsc.data.DisruptorEvent
import java.util.concurrent.Executors
import com.lmax.disruptor.EventHandler
import main.kotlin.com.tritandb.engine.tsc.CompressorFlat
import main.kotlin.com.tritandb.engine.util.BitWriter
import java.io.File
import java.io.OutputStream

/**
 * Created by eugene on 12/05/2017.
 */

val o: OutputStream = File("disruptor.tsc").outputStream()
val b: BitWriter = BitWriter(o)
val c: CompressorFlat = CompressorFlat(System.currentTimeMillis(),b,1)

val handler:EventHandler<DisruptorEvent> = EventHandler({ event, sequence, endOfBatch ->
    val arr:LongArray = longArrayOf(event.value)
    c.addRow(System.currentTimeMillis(),arr)
    println(arr)
})

fun main(args: Array<String>) {
    // Executor that will be used to construct new threads for consumers
    val executor = Executors.newCachedThreadPool()

    // Specify the size of the ring buffer, must be power of 2.
    val bufferSize = 1024

    // Construct the Disruptor
    val disruptor = Disruptor(EventFactory ({ DisruptorEvent() }), bufferSize, executor) //TODO: change to threadfactory

    // Connect the handler
    disruptor.handleEventsWith(handler)

    // Start the Disruptor, starts all threads running
    disruptor.start()

    // Get the ring buffer from the Disruptor to be used for publishing.
    val ringBuffer = disruptor.getRingBuffer()

    ringBuffer.publishEvent { event, sequence -> event.value = 2L }

}