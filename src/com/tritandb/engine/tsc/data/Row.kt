package com.tritandb.engine.tsc.data

/**
 * Created by eugene on 11/05/2017.
 */
data class Row(val timestamp:Long, val values:LongArray) {
    fun getRow():List<Pair> = values.map { value -> Pair(timestamp,value) }
}