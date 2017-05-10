package com.tritandb.engine.tsc

import com.tritandb.engine.util.BitReader
import com.tritandb.engine.util.BitWriter
import java.io.File
import java.io.InputStream
import java.io.OutputStream

/**
 * Created by eugene on 10/05/2017.
 */

fun main(args : Array<String>) {
    val o: OutputStream = File("test.txt").outputStream()
    val b: BitWriter = BitWriter(o)
    b.writeBits(64,1000L)
    o.close()

    val i: InputStream = File("test.txt").inputStream()
    val bi: BitReader = BitReader(i)
    println(bi.readBits(64))
    i.close()
}