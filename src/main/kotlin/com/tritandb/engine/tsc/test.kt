package com.tritandb.engine.tsc

import com.tritandb.engine.query.op.*
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
    for(i in 1..101) {
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
//        println("${measureTimeMillis { range(filePath,start,end) }}")
//        println("${measureTimeMillis { rangeLSM(filePath,start,end) }}")
//        println("${measureTimeMillis { rangeTree(filePath,start,end) }}")
//        println("${measureTimeMillis { rangeHash(filePath,start,end) }}")
    }

}

fun queryTaxi(filePath: String) {
    val fixedSeed = 100L
    val rand = Random(fixedSeed)
    val max = 1467241200
    val min = 1459465200
    val range = (max + 1 - min)
    for(i in 1..101) {
        val a = (rand.nextInt(range)) + min
        val b = (rand.nextInt(range)) + min
        var start = a
        var end = b
        if(a>b) {
            start = b
            end = a
        }
//        println("Time: ${measureTimeMillis { rangeShelburne(filePath,start,end) }}, Start: ${start}, End: ${end}")
//        println("${start},${end},${measureTimeMillis { rangeShelburneTree(filePath,start,end) }}")
        println("${measureTimeMillis { range(filePath,start.toLong(),end.toLong()) }}")
    }

}

fun range(filePath: String, start: Long, end: Long) {
    for(r in RangeFlatChunk(filePath).run(start,end)) {

    }
}