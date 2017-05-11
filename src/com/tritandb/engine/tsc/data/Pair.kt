package com.tritandb.engine.tsc.data

/**
 * Created by eugene on 11/05/2017.
 */
data class Pair(val timestamp:Long, val value:Long) {
    fun getDoubleValue():Double = java.lang.Double.longBitsToDouble(value)
}