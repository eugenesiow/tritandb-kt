package com.tritandb.engine.tsc.data

/**
 * Created by eugene on 12/05/2017.
 */
data class DisruptorEvent(var value:EventProtos.TritanEvent = buildTritanEvent {  }) {
    var arr:MutableList<Long> = mutableListOf()
    var timestamp:Long = 0
}