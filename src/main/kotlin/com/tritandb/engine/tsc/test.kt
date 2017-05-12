package main.kotlin.com.tritandb.engine.tsc

import main.kotlin.com.tritandb.engine.tsc.data.Row
import main.kotlin.com.tritandb.engine.util.BitReader
import main.kotlin.com.tritandb.engine.util.BitWriter
import java.io.File
import java.io.InputStream
import java.io.OutputStream
import java.util.Random

/**
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

    val i:InputStream = File("disruptor.tsc").inputStream()
    val bi:BitReader = BitReader(i)
    val d:DecompressorFlat = DecompressorFlat(bi)
    var r: Row? = null
    var count = 0
    while({ r = d.readRow(); r }() !=null) {
        print("${count++}:")
        println(r)

    }
    i.close()

}