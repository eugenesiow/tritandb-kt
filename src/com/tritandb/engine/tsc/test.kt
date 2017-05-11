package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitReader
import com.tritandb.engine.util.BitWriter
import java.io.File
import java.io.InputStream
import java.io.OutputStream

/**
 * Created by eugene on 10/05/2017.
 */

fun main(args : Array<String>) {
    val o:OutputStream = File("test.tsc").outputStream()
    val b:BitWriter = BitWriter(o)
    val c:CompressorFlat = CompressorFlat(System.currentTimeMillis(),b,1)

    var counter = 1L
    for(x in 1..10000) {
//        c.addValue(System.currentTimeMillis(),Math.random())
        c.addValue(System.currentTimeMillis(),counter++)
    }
    c.close()
    o.close()

    val i:InputStream = File("bedroom_corner-motion.tsc").inputStream()
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