package com.tritandb.engine.tsc

import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitOutput
import main.kotlin.com.tritandb.engine.tsc.CompressorFlat
import java.io.BufferedReader
import java.io.File
import java.io.FileReader
import java.io.OutputStream


/**
 * Created by eugene on 12/05/2017.
 */

fun main(args: Array<String>) {
    val br = BufferedReader(FileReader("/Users/eugene/Documents/Programming/data/shelburne/shelburne_test.csv"))
    br.readLine()
    val o: OutputStream = File("shelburne.tsc").outputStream()
//    val b: BitOutput = BitWriter(o)
    val b: BitOutput = BitByteBufferWriter(com.tritandb.engine.tsc.o)
    val c = CompressorFlat(1271692742103L, b, 6)
    for(line in br.lines()) {
        val parts = line.split(",")
        val arr:MutableList<Long> = mutableListOf()
        for (i in 1..6) {
            arr.add(java.lang.Double.doubleToLongBits(java.lang.Double.parseDouble(parts[i])))
        }
        c.addRow(java.lang.Long.parseLong(parts[0]) / 100000, arr)
    }
    c.close()
    br.close()
}
