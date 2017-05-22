package com.tritandb.engine.tsc

import com.tritandb.engine.experimental.DecompressorFpc
import com.tritandb.engine.util.BitReader
import java.io.File
import java.io.InputStream

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
    readShelburne()
}

fun readShelburne() {
    val i: InputStream = File("data/shelburne_fpc.tsc").inputStream()
    val bi: BitReader = BitReader(i)
    val d: DecompressorFpc = DecompressorFpc(bi)
    var count = 0
    for(r in d.readRows()) {
        print("${count++}:${r!!.timestamp}")
        for(pair in r!!.getRow()) {
            print(", ${pair.getDoubleValue()}")
        }
        println()
    }
    i.close()
}