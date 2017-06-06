package com.tritandb.engine.experimental

import com.tritandb.engine.experimental.valueC.FpcCompressor
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.nio.ByteBuffer
import java.nio.channels.Channels
import kotlin.system.measureTimeMillis

/**
 * TritanDb
 * Created by eugene on 19/05/2017.
 */

fun main(args: Array<String>) {
    println("Time: ${measureTimeMillis{ writeShelburne() }}")
//    readShelburne()
}

fun readShelburne() {
    val c: FpcCompressor = FpcCompressor()
    println(c.decodeVal(File("test.tsc").inputStream()))
}

fun writeSrbench() {
    File("/Users/eugene/Downloads/knoesis_observations_csv_date_sorted/").walkTopDown()
            .filter { !it.name.startsWith(".") && it.isFile && it.name.endsWith(".csv") }.forEach {
        val br = BufferedReader(FileReader(it.absolutePath))
        val header = br.readLine().split(",") //header
        val stationName = it.name.replace(".csv","")
        var previousVals:MutableList<Double> = mutableListOf()
        for(i in 0..header.size-1)
            previousVals.add(0.0)
        var addThis = false
        val channel = Channels.newChannel(File("data_fpc/${stationName}.tsc").outputStream())
        val c: FpcCompressor = FpcCompressor()

        for(line in br.lines()) {
            addThis = true
            val parts = line.split(",")
            if(parts.size>header.size-1) {
                val doubles: DoubleArray = DoubleArray(header.size-1)
                for(i in 0..header.size-1) {
                    if (parts[i+1] == "")
                        doubles[i] = previousVals[i]
                    else {
                        var value:Double = 0.0
                        if(parts[i]=="true")
                            value = 1.0
                        else if(parts[i]=="false")
                            value = 0.0
                        else
                            value = parts[i+1].toDouble()
                        doubles[i] = value
                        previousVals[i] = value
                    }
                }
                val bb = ByteBuffer.allocate(256)
                c.compress(bb, doubles)
                bb.flip()
                channel.write(bb)
            } else {
                println(line)
            }
        }
        channel.close()
        br.close()

    }
}

fun writeShelburne() {
    val br = BufferedReader(FileReader("/Users/eugene/Documents/Programming/data/shelburne/shelburne.csv"))
    br.readLine() //header
    val channel = Channels.newChannel(File("test.tsc").outputStream())
    val c: FpcCompressor = FpcCompressor()
    for(line in br.lines()) {
        val parts = line.split(",")
        if(parts.size>6) {
            val doubles: DoubleArray = DoubleArray(6)
            for (i in 0..5) {
                if (parts[i+1] == "" || parts[i+1]=="NaN")
                    continue
                doubles[i] = parts[i+1].toDouble()
            }
            val bb = ByteBuffer.allocate(256)
            c.compress(bb, doubles)
            bb.flip()
            channel.write(bb)
        }
    }
    channel.close()
    br.close()
}