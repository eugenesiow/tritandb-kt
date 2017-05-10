package com.tritandb.engine.tsc

import com.tritandb.engine.util.BitWriter
import java.io.File
import java.io.OutputStream

/**
 * Created by eugene on 10/05/2017.
 */

fun main(args : Array<String>) {
    val o : OutputStream = File("test.txt").outputStream()
    val b : BitWriter = BitWriter(o)
    b.writeBits(64,1000L)
    o.close()
}