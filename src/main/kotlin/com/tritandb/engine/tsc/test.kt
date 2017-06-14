package com.tritandb.engine.tsc

import com.tritandb.engine.experimental.valueC.DecompressorFpc
import com.tritandb.engine.experimental.timestampC.DecompressorTs
import com.tritandb.engine.query.op.RangeFlat
import com.tritandb.engine.query.op.RangeTree
import com.tritandb.engine.util.BitReader
import java.io.File
import java.io.InputStream
import java.util.*
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
//    println("Time: ${measureTimeMillis{readDelta("data/shelburne.tsc")}}")
//    println("Time: ${measureTimeMillis{readShelburne("data/shelburne.tsc")}}")

    queryShelburne("data/shelburne.tsc")

//    println("Time: ${measureTimeMillis{readShelburneFPC("data/shelburne_fpc.tsc")}}")
}

fun queryShelburne(filePath: String) {
    val fixedSeed = 100L
    val rand = Random(fixedSeed)
    val max = 1406141325958
    val min = 1271692742104
    val range = ((max + 1 - min )/100).toInt()
    for(i in 1..21) {
        val a = (rand.nextInt(range))*100L + min
        val b = (rand.nextInt(range))*100L + min
        var start = a
        var end = b
        if(a>b) {
            start = b
            end = a
        }
//        println("Time: ${measureTimeMillis { rangeShelburne(filePath,start,end) }}, Start: ${start}, End: ${end}")
//        println("${start},${end},${measureTimeMillis { rangeShelburneTree(filePath,start,end) }}")
//        println("${measureTimeMillis { rangeShelburne(filePath,start,end) }}")
        println("${measureTimeMillis { rangeShelburneTree(filePath,start,end) }}")
    }

}

fun rangeShelburneTree(filePath: String, start: Long, end: Long) {
    for(r in RangeTree(filePath).run(start,end)) {

    }
}

fun rangeShelburne(filePath: String, start: Long, end: Long) {
    for(r in RangeFlat(filePath).run(start,end)) {

    }
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

fun readShelburneToFile(filePath:String) {
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

fun readShelburne(filePath:String) {
    val i: InputStream = File(filePath).inputStream()
    val bi: BitReader = BitReader(i)
    val d: DecompressorFlat = DecompressorFlat(bi)

    for (r in d.readRows()) {
        print("${r.timestamp}")
        for (pair in r.getRow()) {
            print(", ${pair.getDoubleValue()}")
        }
        println()
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