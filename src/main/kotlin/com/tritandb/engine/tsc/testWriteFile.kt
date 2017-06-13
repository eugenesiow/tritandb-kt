package com.tritandb.engine.tsc

import com.tritandb.engine.experimental.valueC.CompressorFpDeltaDelta
import com.tritandb.engine.experimental.valueC.CompressorFpc
import com.tritandb.engine.experimental.valueC.DecompressorFpDeltaDelta
import com.tritandb.engine.experimental.valueC.DecompressorFpc
import com.tritandb.engine.tsc.data.EventProtos
import com.tritandb.engine.tsc.data.buildRow
import com.tritandb.engine.tsc.data.buildRows
import com.tritandb.engine.tsc.data.buildTritanEvent
import com.tritandb.engine.util.BitByteBufferReader
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitInput
import com.tritandb.engine.util.BitOutput
import org.zeromq.ZMQ
import java.io.*
import java.lang.Double
import java.text.SimpleDateFormat
import kotlin.system.measureTimeMillis

/**
* TritanDb
* Created by eugene on 10/05/2017.
*/

fun main(args : Array<String>) {
    var outputFilePath = "data/shelburne.tsc"
    var filePath = "/Users/eugene/Documents/Programming/data/shelburne/shelburne.csv"
//    var filePath = "/Users/eugene/Documents/Programming/data/shelburne/shelburne_test.csv"

//    println("Write gor tree")
////    for(x in 1..10) {
//        val c: CompressorTree = CompressorTree(outputFilePath, 6)
//        println("${measureTimeMillis { writeFileShelburneTree(filePath, c) }}")
////    }

    println("Read gor tree")
//    for(x in 1..10) {
        val d: DecompressorTree = DecompressorTree(outputFilePath)
        println("${measureTimeMillis { readFileShelburneTree(outputFilePath, d) }}")
//    }


//    println("Write gor")
//    for(x in 1..10) {
//        val o: OutputStream = File(outputFilePath).outputStream()
//        val out: BitOutput = BitByteBufferWriter(o)
//        val c: Compressor = CompressorFlat(0, out, 6)
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
//        c.close()
//    }

//    println("Read gor")
//    for(x in 1..10) {
//        var i: InputStream = File(outputFilePath).inputStream()
//        var bi: BitInput = BitByteBufferReader(i)
//        var d: Decompressor = DecompressorFlat(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

//    println("Write FPC Delta")
//    for(x in 1..10) {
//        val o = File(outputFilePath).outputStream()
//        val out = BitByteBufferWriter(o)
//        val c = CompressorFpDeltaDelta(0, out, 6)
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
//        c.close()
//    }

//    println("Read FPC Delta")
//    for(x in 1..10) {
//        var i: InputStream = File(outputFilePath).inputStream()
//        var bi: BitInput = BitByteBufferReader(i)
//        var d: Decompressor = DecompressorFpDeltaDelta(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

//
//    println("Write FPC")
//    for(x in 1..10) {
//        val o = File(outputFilePath).outputStream()
//        val out = BitByteBufferWriter(o)
//        val c = CompressorFpc(0, out, 6)
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
//        c.close()
//    }

//    println("Read FPC")
//    for(x in 1..10) {
//        i = File(outputFilePath).inputStream()
//        bi = BitByteBufferReader(i)
//        d = DecompressorFpc(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

    outputFilePath = "data/taxi.tsc"
    filePath = "/Users/eugene/Documents/Programming/data/2016_green_taxi_trip_data_sorted.csv"

//    println("Write gor")
//    for(x in 1..10) {
//        val o: OutputStream = File(outputFilePath).outputStream()
//        val out: BitOutput = BitByteBufferWriter(o)
//        val c: Compressor = CompressorFlat(0, out, 20)
//        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
//        c.close()
//    }
//
//    println("Read gor")
//    for(x in 1..10) {
//        var i: InputStream = File(outputFilePath).inputStream()
//        var bi: BitInput = BitByteBufferReader(i)
//        var d: Decompressor = DecompressorFlat(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

//    println("Write FPC Delta")
//    for(x in 1..10) {
//        val o = File(outputFilePath).outputStream()
//        val out = BitByteBufferWriter(o)
//        val c = CompressorFpDeltaDelta(0, out, 20)
//        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
//        c.close()
//    }
//
//    println("Read FPC Delta")
//    for(x in 1..10) {
//        val i: InputStream = File(outputFilePath).inputStream()
//        val bi: BitInput = BitByteBufferReader(i)
//        val d: Decompressor = DecompressorFpDeltaDelta(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }
//
//    println("Write FPC")
//    for(x in 1..10) {
//        val o = File(outputFilePath).outputStream()
//        val out = BitByteBufferWriter(o)
//        val c = CompressorFpc(0, out, 20)
//        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
//        c.close()
//    }
//
//    println("Read FPC")
//    for(x in 1..10) {
//        val i = File(outputFilePath).inputStream()
//        val bi = BitByteBufferReader(i)
//        val d = DecompressorFpc(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

    outputFilePath = "data/"
    filePath = "/Users/eugene/Downloads/knoesis_observations_csv_date_sorted/"
//    filePath = "/Users/eugene/Downloads/knoesis_empty/"

//    println("Write gor")
////    for(x in 1..10) {
//        println("${measureTimeMillis { writeFileSrBench(filePath,outputFilePath,"gor") }}")
////    }
//
//    println("Read gor")
//    for(x in 1..10) {
//        println("${measureTimeMillis { readFileSrBench(outputFilePath,"gor") }}")
//    }

//    println("Write FP Delta")
////    for(x in 1..10) {
//        println("${measureTimeMillis { writeFileSrBench(filePath,outputFilePath,"fpdelta") }}")
//    }
//
//    println("Read FP Delta")
//    for(x in 1..10) {
//        println("${measureTimeMillis { readFileSrBench(outputFilePath,"fpdelta") }}")
//    }
//
//    println("Write FPC")
//    for(x in 1..10) {
//        println("${measureTimeMillis { writeFileSrBench(filePath,outputFilePath,"fpc") }}")
//    }
//
//    println("Read FPC")
//    for(x in 1..10) {
//        println("${measureTimeMillis { readFileSrBench(outputFilePath,"fpc") }}")
//    }

}

fun writeFileShelburneTree(filePath:String,c:CompressorTree) {
    val br = BufferedReader(FileReader(filePath))
    br.readLine() //header
    for(line in br.lines()) {
        var addThis = true
        val parts = line.split(",")
        if(parts.size>6) {
            val list = mutableListOf<Long>()
            for(i in 1..6) {
                if (parts[i] == "")
                    addThis = false
                else
                    list.add(Double.doubleToLongBits(parts[i].toDouble()))
            }

            if(addThis)
                c.addRow((parts[0].toLong() / 1000000),list)
        }
    }
    br.close()
    c.close()
}

fun writeFileShelburne(filePath:String,c:Compressor) {
    val br = BufferedReader(FileReader(filePath))
    br.readLine() //header
    for(line in br.lines()) {
        var addThis = true
        val parts = line.split(",")
        if(parts.size>6) {
            val list = mutableListOf<Long>()
            for(i in 1..6) {
                if (parts[i] == "")
                    addThis = false
                else
                    list.add(Double.doubleToLongBits(parts[i].toDouble()))
            }

            if(addThis)
                c.addRow((parts[0].toLong() / 1000000),list)
        }
    }
    br.close()
    c.close()
}

fun readFileSrBench(folderPath:String,cType:String) {
    File(folderPath).walkTopDown()
            .filter { !it.name.startsWith(".") && it.isFile && it.name.endsWith(".tsc") }.forEach {
        val i: InputStream = File(it.absolutePath).inputStream()
        val bi: BitInput = BitByteBufferReader(i)
//        println(it.absolutePath)
        var d:Decompressor = DecompressorFlat(bi)
        when(cType) {
            "gor" ->  d = DecompressorFlat(bi)
            "fpc" -> d = DecompressorFpc(bi)
            "fpdelta" -> d = DecompressorFpDeltaDelta(bi)
        }
        File("${it.absolutePath}.csv").printWriter().use { out ->
            for (r in d.readRows()) {
//                out.print("${r.timestamp}")
                for (pair in r.getRow()) {
//                    out.print(", ${pair.getDoubleValue()}")
                }
//                out.println()
            }
        }
        i.close()
    }
}

fun writeFileSrBench(filePath:String, outputFilePath:String,cType:String) {
    val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val sdf = SimpleDateFormat(DATE_FORMAT)
    File(filePath).walkTopDown()
            .filter { !it.name.startsWith(".") && it.isFile && it.name.endsWith(".csv") }.forEach {
        val br = BufferedReader(FileReader(it.absolutePath))
        val header = br.readLine().split(",") //header
        val stationName = it.name.replace(".csv","")
        val o: OutputStream = File(outputFilePath + stationName + ".tsc").outputStream()
        val out: BitOutput = BitByteBufferWriter(o)
        val previousVals:MutableList<Long> = mutableListOf()
        for(i in 0..header.size-1)
            previousVals.add(0)
        var c: Compressor = CompressorFlat(0, out, header.size-1)
        when(cType) {
            "gor" ->  c = CompressorFlat(0, out, header.size-1)
            "fpc" -> c = CompressorFpc(0, out, header.size-1)
            "fpdelta" -> c = CompressorFpDeltaDelta(0, out, header.size-1)
        }
        var addThis = false
        for(line in br.lines()) {
            addThis = true
            val parts = line.split(",")
            if(parts.size>header.size-1) {
                val list = mutableListOf<Long>()
                val timestamp = (sdf.parse(parts[0]).time/1000)
                for(i in 1..header.size-1) {
                    if (parts[i] == "")
                        list.add(previousVals[i])
                    else {
                        var value:Long = 0
                        if(parts[i]=="true")
                            value = 1L
                        else if(parts[i]=="false")
                            value = 0L
                        else
                            value = java.lang.Double.doubleToLongBits(parts[i].toDouble())
                        list.add(value)
                        previousVals[i] = value
                    }

                }
                c.addRow(timestamp,list)
            } else {
                println(line)
            }
        }
        br.close()
        c.close()
    }
}

fun writeFileTaxi(filePath:String, c:Compressor) {
    val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val sdf = SimpleDateFormat(DATE_FORMAT)
    val br = BufferedReader(FileReader(filePath))
    br.readLine() //header
    for(line in br.lines()) {
        val parts = line.split(",")
        if(parts.size>21) {
            val timestamp = (sdf.parse(parts[1]).time/1000)
            val list = mutableListOf<Long>()
            list.add(parts[0].toLong())
            list.add(sdf.parse(parts[2]).time/1000)
            if(parts[3]=="true")
                list.add(1)
            else
                list.add(0)
            list.add(parts[4].toLong())
            list.add(Double.doubleToLongBits(parts[5].toDouble()))
            list.add(Double.doubleToLongBits(parts[6].toDouble()))
            list.add(Double.doubleToLongBits(parts[7].toDouble()))
            list.add(Double.doubleToLongBits(parts[8].toDouble()))
            list.add(parts[9].toLong())
            list.add(Double.doubleToLongBits(parts[10].toDouble()))
            list.add(Double.doubleToLongBits(parts[11].toDouble()))
            list.add(Double.doubleToLongBits(parts[12].toDouble()))
            list.add(Double.doubleToLongBits(parts[13].toDouble()))
            list.add(Double.doubleToLongBits(parts[14].toDouble()))
            list.add(Double.doubleToLongBits(parts[15].toDouble()))
            list.add(Double.doubleToLongBits(parts[17].toDouble()))
            list.add(Double.doubleToLongBits(parts[18].toDouble()))
            list.add(parts[19].toLong())
            if(parts[20].isEmpty())
                list.add(1)
            else
                list.add(parts[20].toLong())
            list.add(sdf.parse(parts[21]).time/1000)
            c.addRow(timestamp,list)
        }
    }
    br.close()
    c.close()
}

fun readFileShelburne(filePath:String, d:Decompressor) { //works for each
//    var count = 0
    File("${filePath}.csv").printWriter().use { out ->
        for (r in d.readRows()) {
//            out.print("${r.timestamp}")
            for (pair in r.getRow()) {
//                out.print(", ${pair.getDoubleValue()}")
            }
//            out.println()
        }
    }
}

fun readFileShelburneTree(filePath:String, d:DecompressorTree) { //works for each
    var count = 0
//    File("${filePath}.csv").printWriter().use { out ->
        for (r in d.readRows()) {
            print("${count++}:${r.timestamp}")
//            out.print("${r.timestamp}")
            for (pair in r.getRow()) {
                print(", ${pair.getDoubleValue()}")
//                out.print(", ${pair.getDoubleValue()}")
            }
            println()
//            out.println()
        }
//    }
}