package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType.CLOSE
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType.INSERT
import com.tritandb.engine.tsc.data.buildRow
import com.tritandb.engine.tsc.data.buildRows
import com.tritandb.engine.tsc.data.buildTritanEvent
import org.zeromq.ZMQ
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.text.SimpleDateFormat
import kotlin.system.measureTimeMillis


/**
* TritanDb
* Created by eugene on 12/05/2017.
*/

fun main(args: Array<String>) {
    val context = ZMQ.context(2)
    val sender = context.socket(ZMQ.PUSH)
    sender.connect("tcp://localhost:5700")

    Thread.sleep(1000)

    println("Time: ${measureTimeMillis{shelburne(sender)}}")
//    println("Time: ${measureTimeMillis{ srbench(sender) }}")

    sender.close()
    context.close()
}

fun srbench(sender:ZMQ.Socket) {
    val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val sdf = SimpleDateFormat(DATE_FORMAT)
    File("/Users/eugene/Downloads/knoesis_observations_csv_date_sorted/").walkTopDown()
            .filter { !it.name.startsWith(".") && it.isFile && it.name.endsWith(".csv") }.forEach {
        val br = BufferedReader(FileReader(it.absolutePath))
        val header = br.readLine().split(",") //header
        val stationName = it.name.replace(".csv","")
        var previousVals:MutableList<Long> = mutableListOf()
        for(i in 0..header.size-1)
            previousVals.add(0)
        var addThis = false
        for(line in br.lines()) {
            addThis = true
            val parts = line.split(",")
            if(parts.size>header.size-1) {
                val event = buildTritanEvent {
                    type = INSERT
                    name = stationName
                    rows = buildRows {
                        addRow(buildRow {
                            timestamp = (sdf.parse(parts[0]).time/1000)
                            for(i in 1..header.size-1) {
                                if (parts[i] == "")
                                    addValue(previousVals[i])
                                else {
                                    var value:Long = 0
                                    if(parts[i]=="true")
                                        value = 1L
                                    else if(parts[i]=="false")
                                        value = 0L
                                    else
                                        value = java.lang.Double.doubleToLongBits(parts[i].toDouble())
                                    addValue(value)
                                    previousVals[i] = value
                                }
                            }
                        })
                    }
                }
                sender.send(event.toByteArray())
            } else {
                println(line)
            }
        }
        br.close()
        if(addThis) {
            sender.send(buildTritanEvent {
                type = CLOSE
                name = stationName
            }.toByteArray())
        }
    }
}

fun shelburne(sender:ZMQ.Socket) {
    val br = BufferedReader(FileReader("/Users/eugene/Documents/Programming/data/shelburne/shelburne.csv"))
    br.readLine() //header
    for(line in br.lines()) {
        var addThis = true
        val parts = line.split(",")
        if(parts.size>6) {
            val event = buildTritanEvent {
                type = INSERT
                name = "shelburne_fpc"
                rows = buildRows {
                    addRow(buildRow {
                        timestamp = (parts[0].toLong() / 1000000)
                        for(i in 1..6) {
                            if (parts[i] == "")
                                addThis = false
                            else
                                addValue(java.lang.Double.doubleToLongBits(parts[i].toDouble()))
                        }
                    })
                }
            }
            if(addThis)
                sender.send(event.toByteArray())
        }
    }
    br.close()

    sender.send(buildTritanEvent {
        type = CLOSE
        name = "shelburne_fpc"
    }.toByteArray())
}
