package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType.CLOSE
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType.INSERT
import com.tritandb.engine.tsc.data.buildRow
import com.tritandb.engine.tsc.data.buildRows
import com.tritandb.engine.tsc.data.buildTritanEvent
import org.zeromq.ZMQ
import java.io.BufferedReader
import java.io.FileReader
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

    sender.close()
    context.close()
}

fun shelburne(sender:ZMQ.Socket) {


    val br = BufferedReader(FileReader("/Users/eugene/Documents/Programming/data/shelburne/shelburne_test.csv"))
    br.readLine() //header
    for(line in br.lines()) {
        var addThis = true
        val parts = line.split(",")
        if(parts.size>6) {
            val event = buildTritanEvent {
                type = INSERT
                name = "shelburne"
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

    sender.send(buildTritanEvent {
        type = CLOSE
        name = "shelburne"
    }.toByteArray())
}
