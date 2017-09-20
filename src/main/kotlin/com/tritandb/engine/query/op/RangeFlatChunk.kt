package com.tritandb.engine.query.op

import com.tritandb.engine.tsc.DecompressorFlatChunk
import com.tritandb.engine.tsc.data.Row
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 29/06/2017.
 */
class RangeFlatChunk(val filePath:String):TrOp {
    var start: Long = 0
    var end: Long = 0
    val cols = mutableListOf<String>()
    override var iterator:Iterator<Row> = buildIterator {}
    val aggregates = mutableMapOf<String,MutableList<Pair<String,String>>>()

    override fun execute() {
//        println("$start:$end:$cols")
        iterator = run(start,end)
    }

    fun aggr(col: String, varName: String, aggrName: String) {
        var aggregate = aggregates[col]
        if(aggregate==null)
            aggregate = mutableListOf<Pair<String,String>>()
        aggregate.add(Pair(aggrName,varName))
        aggregates.put(col,aggregate)
    }

    fun avgRun(start: Long, end: Long, col:Int): Iterator<Row> = buildIterator {
        val d: DecompressorFlatChunk = DecompressorFlatChunk(filePath)
        for (r in d.readAggr(start,end,col)) {
            yield(r)
        }
        d.close()
    }

    fun run(start: Long, end: Long): Iterator<Row> = buildIterator {
        val d: DecompressorFlatChunk = DecompressorFlatChunk(filePath)
        for (r in d.readRange(start,end)) {
            if(r.timestamp>=start) {
                yield(r)
                if(r.timestamp>end) {
                    break
                }
            }
        }
        d.close()
    }
}