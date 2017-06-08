package com.tritandb.engine.tsc

import com.tritandb.engine.experimental.valueC.CompressorFpDeltaDelta
import com.tritandb.engine.experimental.valueC.CompressorFpc
import com.tritandb.engine.experimental.valueC.DecompressorFpDeltaDelta
import com.tritandb.engine.experimental.valueC.DecompressorFpc
import com.tritandb.engine.util.BitByteBufferReader
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitInput
import com.tritandb.engine.util.BitOutput
import java.io.*
import java.lang.Double
import kotlin.system.measureTimeMillis

/**
* TritanDb
* Created by eugene on 10/05/2017.
*/

fun main(args : Array<String>) {
    val outputFilePath = "data/shelburne.tsc"
//    val filePath = "/Users/eugene/Documents/Programming/data/shelburne/shelburne.csv"
    val filePath = "/Users/eugene/Documents/Programming/data/shelburne/shelburne_test.csv"

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

    println("Write FPC Delta")
//    for(x in 1..10) {
        val o = File(outputFilePath).outputStream()
        val out = BitByteBufferWriter(o)
        val c = CompressorFpDeltaDelta(0, out, 6)
        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
        c.close()
//    }
//
    println("Read FPC Delta")
//    for(x in 1..10) {
        var i: InputStream = File(outputFilePath).inputStream()
        var bi: BitInput = BitByteBufferReader(i)
        var d: Decompressor = DecompressorFpDeltaDelta(bi)
        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
        i.close()
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

}

fun writeFileShelburne(filePath:String,c:Compressor) {
    val br = BufferedReader(FileReader(filePath))
    br.readLine() //header
    for(line in br.lines()) {
        var addThis = true
        val parts = line.split(",")
        if(parts.size>6) {
            var list = mutableListOf<Long>()
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

fun readFileShelburne(filePath:String, d:Decompressor) {
//    var count = 0
    File("${filePath}.csv").printWriter().use { out ->
        for (r in d.readRows()) {
            out.print("${r.timestamp}")
            for (pair in r.getRow()) {
                out.print(", ${pair.getDoubleValue()}")
            }
            out.println()
        }
    }
}