package com.tritandb.engine.tsc

/**
 * TritanDb
 * Created by eugene on 19/05/2017.
 */
interface Compressor {
    fun addRow(timestamp:Long, values:List<Long>)
    fun close()
}