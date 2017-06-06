package com.tritandb.engine.tsc

import com.tritandb.engine.experimental.valueC.DecompressorFpc
import com.tritandb.engine.experimental.timestampC.DecompressorTs
import com.tritandb.engine.util.BitReader
import java.io.File
import java.io.InputStream
import kotlin.system.measureTimeMillis

/**
* TritanDb
* Created by eugene on 10/05/2017.
*/

fun main(args : Array<String>) {
//    val i: InputStream = File("data/4UT01.tsc").inputStream()
//    val bi: BitReader = BitReader(i)
//    val d: DecompressorFlat = DecompressorFlat(bi)
////    var r: Row? = null
//    var count = 0
////    while({ r = d.readRow(); r }() !=null) {
//    for(r in d.readRows()) {
//        print("${count++}:${r!!.timestamp}")
////        print("${r!!.timestamp}")
//        for(pair in r!!.getRow()) {
//            print(", ${pair.getDoubleValue()}")
//        }
//        println()
//    }
//    i.close()
    println("Time: ${measureTimeMillis{readDelta("data/shelburne.tsc")}}")
//    println("Time: ${measureTimeMillis{readShelburne("data/shelburne.tsc")}}")
//    println("Time: ${measureTimeMillis{readShelburneFPC("data/shelburne_fpc.tsc")}}")
}

fun readDelta(filePath:String) {
    val i: InputStream = File(filePath).inputStream()
    val bi: BitReader = BitReader(i)
    val d: DecompressorTs = DecompressorTs(bi)
    var count = 0
//    File("${filePath}.csv").printWriter().use { out ->
        for (r in d.readRows()) {
            print("${count++}:${r.timestamp}")
//            if(count>40)
//                break
            for (pair in r.getRow()) {
                print(", ${pair.getDoubleValue()}")
            }
            println()
        }
//    }
    i.close()
}

fun readShelburne(filePath:String) {
    val i: InputStream = File(filePath).inputStream()
    val bi: BitReader = BitReader(i)
    val d: DecompressorFlat = DecompressorFlat(bi)
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
    i.close()
}

fun readShelburneFPC(filePath:String) {
    val i: InputStream = File(filePath).inputStream()
    val bi: BitReader = BitReader(i)
    val d: DecompressorFpc = DecompressorFpc(bi)
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
    i.close()
}