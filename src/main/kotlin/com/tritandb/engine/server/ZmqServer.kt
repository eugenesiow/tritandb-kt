package com.tritandb.engine.server

import com.lmax.disruptor.EventFactory
import com.lmax.disruptor.EventHandler
import com.lmax.disruptor.dsl.Disruptor
import com.lmax.disruptor.util.DaemonThreadFactory
import com.natpryce.konfig.*
import com.tritandb.engine.query.engine.QueryExecutor
import com.tritandb.engine.tsc.Compressor
import com.tritandb.engine.tsc.CompressorFlatChunk
import com.tritandb.engine.tsc.data.DisruptorEvent
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType.*
import mu.KLogging
import org.zeromq.ZMQ
import java.io.File


/**
 * TritanDb
 * Created by eugene on 17/05/2017.
 */
class ZmqServer(private val config:Configuration) {

    companion object: KLogging()
//    data class FileCompressor(val compressor: Compressor)

    private object server : PropertyGroup() {
        val port by intType
        val host by stringType
        val bufferSize by intType
        val dataDir by stringType
    }

    private val q = QueryExecutor(config)

//    private val C:MutableMap<String, FileCompressor> = mutableMapOf()
    private val C:MutableMap<String, Compressor> = mutableMapOf()

    private val handler: EventHandler<DisruptorEvent> = EventHandler({ (tEvent), _, _ ->
        when(tEvent.type) {
            INSERT -> {
                if(tEvent.hasRows()) {
                    val firstRow = tEvent.rows.getRow(0)
                    CreateMetadata(tEvent.name,firstRow.valueCount)
                    val c = GetCompressor(tEvent.name,firstRow.timestamp,firstRow.valueCount)
                    for (row in tEvent.rows.rowList) {
                        c.addRow(row.timestamp, row.valueList)
                    }
                }
            }
            QUERY -> {
                ProcessQuery(q,tEvent.name!!,tEvent.address!!)
//                if(tEvent.hasRows()) {
//                    val firstRow = tEvent.rows.getRow(0)
//                    var col = -1
//                    var aggregation = -1
//                    if(tEvent.rows.rowCount>1) {
//                        col = tEvent.rows.getRow(1).getValue(0).toInt()
//                    }
//                    if(tEvent.rows.rowCount>2) {
//                        aggregation = tEvent.rows.getRow(2).getValue(0).toInt()
//                    }
//                    val context = ZMQ.context(1)
//                    val sender = context.socket(ZMQ.PUSH)
//                    sender.connect("tcp://${tEvent.address}:5800")
////                    var i = 0
////                    var row = ""
//                    val range = RangeFlatChunk("${config[server.dataDir]}/${tEvent.name}.tsc")
//                    var rangeRun = range.run(firstRow.timestamp,firstRow.getValue(0))
//                    if(aggregation>0) {
//                        rangeRun = range.avgRun(firstRow.timestamp,firstRow.getValue(0),col)
//                    }
//                    for((timestamp, values) in rangeRun) {
//                        var row = ""
//                        if(aggregation>0) {
//                            row += "${values[0]}"
//                        } else {
//                            row += timestamp.toString()
//                            if (col == -1) {
//                                for (value in values)
//                                    row += ",$value"
//                            } else {
//                                row += ",${values[col]}"
//                            }
//                        }
//                        sender.send(row)
//                    }
//                    sender.send("end")
//                    sender.close()
//                }
            }
            INSERT_META -> {
                when(tEvent.name) {
                    "list" -> SendStr(ListTimeSeries(),tEvent.address)
                    else -> println(tEvent.name)
                }
            }
            CLOSE -> {
                val c = C.getValue(tEvent.name)
                c.close()
                C.remove(tEvent.name)
                println("closed")
            }
            else -> {
            }
        }
    })

    private fun CreateMetadata(name:String, width:Int) {
        //check if tsc file exists
        val tscFileExists = File("${config[server.dataDir]}/$name.tsc").exists()
        if(!tscFileExists) {
            val jsonMetadata = File("${config[server.dataDir]}/$name.json")
            //TODO: create json metadata and mappings file automatically
        }
    }

    private fun ProcessQuery(q:QueryExecutor, query: String, address: String) {
        val context = ZMQ.context(1)
        val sender = context.socket(ZMQ.PUSH)
        sender.connect("tcp://$address:5800")
        val rangeRun = q.query(query)
        for((timestamp, values) in rangeRun) {
            var row = ""
            row += timestamp.toString()
            for (value in values)
                row += ",${java.lang.Double.longBitsToDouble(value)}"
            sender.send(row)
        }
        sender.send("end")
        sender.close()
    }

    private fun SendStr(str: String, address:String) {
        val context = ZMQ.context(1)
        val sender = context.socket(ZMQ.PUSH)
        sender.connect("tcp://$address:5800")
        sender.send(str)
        sender.close()
    }

    private fun ListTimeSeries():String {
        //list the timeseries
        var listing = ""
        File(config[server.dataDir]).walk().forEach {
            if(it.name.endsWith(".tsc")) {
                listing += it.name.replace(".tsc","") + "\n"
            }
        }
        return listing
    }

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

        logger.info("server started...")
        while (!Thread.currentThread().isInterrupted) {
            val msg = receiver.recv()
            ringBuffer.publishEvent { event, _ -> event.value = TritanEvent.parseFrom(msg)}
        }
    }
}