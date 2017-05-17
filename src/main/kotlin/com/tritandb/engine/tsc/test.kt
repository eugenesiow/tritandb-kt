package com.tritandb.engine.tsc

import com.tritandb.engine.util.BitReader
import java.io.File
import java.io.InputStream

/**
* TritanDb
* Created by eugene on 10/05/2017.
*/

fun main(args : Array<String>) {
//    val o:OutputStream = File("test.tsc").outputStream()
//    val b:BitWriter = BitWriter(o)
//    val c:CompressorFlat = CompressorFlat(System.currentTimeMillis(),b,1)
//    val rand:Random = Random()
//    val MAX_RAND_INT_SIZE = 10000
//
//    for(x in 1..10000) {
//        val arr:LongArray = longArrayOf(rand.nextInt(MAX_RAND_INT_SIZE).toLong())
//        c.addRow(System.currentTimeMillis(),arr)
//    }
//    c.close()
//    o.close()

    val i: InputStream = File("shelburne.tsc").inputStream()
    val bi: BitReader = BitReader(i)
    val d: DecompressorFlat = DecompressorFlat(bi)
//    var r: Row? = null
    var count = 0
//    while({ r = d.readRow(); r }() !=null) {
    for(r in d.readRows()) {
        print("${count++}:${r!!.timestamp}")
//        print("${r!!.timestamp}")
        for(pair in r!!.getRow()) {
            print(", ${pair.getDoubleValue()}")
        }
        println()
    }
    i.close()

}