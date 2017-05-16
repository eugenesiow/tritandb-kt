package com.tritandb.engine.tsc

import main.kotlin.com.tritandb.engine.tsc.CompressorFlat
import main.kotlin.com.tritandb.engine.util.BitWriter
import java.io.*


/**
 * Created by eugene on 12/05/2017.
 */

fun main(args: Array<String>) {
    val br = BufferedReader(FileReader("/Users/eugene/Documents/Programming/data/shelburne/shelburne_test.csv"))
    br.readLine()
    val o: OutputStream = File("shelburne.tsc").outputStream()
    val b: BitWriter = BitWriter(o)
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
