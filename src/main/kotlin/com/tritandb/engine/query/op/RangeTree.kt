package com.tritandb.engine.query.op

import com.tritandb.engine.tsc.Decompressor
import com.tritandb.engine.tsc.DecompressorFlatChunk
import com.tritandb.engine.tsc.DecompressorHash
import com.tritandb.engine.tsc.DecompressorTree
import com.tritandb.engine.tsc.data.Row
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 13/06/2017.
 */
class RangeTree(val filePath:String) {

    fun run(start: Long, end: Long): Iterator<Row> = buildIterator {
        val d: DecompressorTree = DecompressorTree(filePath)
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