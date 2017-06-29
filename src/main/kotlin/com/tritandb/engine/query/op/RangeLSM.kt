package com.tritandb.engine.query.op

import com.tritandb.engine.tsc.DecompressorLSMTree
import com.tritandb.engine.tsc.data.Row
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 29/06/2017.
 */
class RangeLSM (val filePath:String) {

    fun run(start: Long, end: Long): Iterator<Row> = buildIterator {
        val d: DecompressorLSMTree = DecompressorLSMTree(filePath)
        for (r in d.readRange(start,end)) {
            if(r.timestamp>=start) {
                yield(r)
                if(r.timestamp>end) {
                    d.close()
                    break
                }
            }
        }
    }
}