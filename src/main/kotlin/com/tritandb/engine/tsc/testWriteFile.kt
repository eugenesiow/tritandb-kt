package com.tritandb.engine.tsc

import com.tritandb.engine.experimental.valueC.DecompressorFpc
import com.tritandb.engine.experimental.timestampC.DecompressorTs
import com.tritandb.engine.experimental.valueC.CompressorFpDeltaDelta
import com.tritandb.engine.experimental.valueC.CompressorFpc
import com.tritandb.engine.tsc.data.EventProtos
import com.tritandb.engine.tsc.data.buildRow
import com.tritandb.engine.tsc.data.buildRows
import com.tritandb.engine.tsc.data.buildTritanEvent
import com.tritandb.engine.util.*
import java.io.*
import java.lang.Double
import kotlin.system.measureTimeMillis

/**
* TritanDb
* Created by eugene on 10/05/2017.
*/

fun main(args : Array<String>) {
    val outputFilePath = "data/shelburne.tsc"
    val filePath = "/Users/eugene/Documents/Programming/data/shelburne/shelburne.csv"
//    val filePath = "/Users/eugene/Documents/Programming/data/shelburne/shelburne_test.csv"

//    var o: OutputStream = File(outputFilePath).outputStream()
//    var out: BitOutput = BitByteBufferWriter(o)
//    var c: Compressor = CompressorFlat(0,out,6)
//    println("${measureTimeMillis{writeFileShelburne(filePath,c)}}")
//    c.close()

    val i: InputStream = File(outputFilePath).inputStream()
    val bi: BitInput = BitByteBufferReader(i)
    val d: DecompressorFlat = DecompressorFlat(bi)
    println("${measureTimeMillis{readFileShelburne(outputFilePath,d)}}")
    i.close()
//
//    o = File(outputFilePath).outputStream()
//    out = BitByteBufferWriter(o)
//    c = CompressorFpDeltaDelta(0,out,6)
//    println("${measureTimeMillis{writeFileShelburne(filePath,c)}}")
//    c.close()
//    o = File(outputFilePath).outputStream()
//    out = BitByteBufferWriter(o)
//    c = CompressorFpc(0,out,6)
//    println("${measureTimeMillis{writeFileShelburne(filePath,c)}}")
//    c.close()


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