package com.tritandb.engine.query.op

import com.tritandb.engine.tsc.DecompressorFlat
import com.tritandb.engine.tsc.DecompressorFlatChunk
import com.tritandb.engine.tsc.DecompressorTree
import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitByteBufferReader
import com.tritandb.engine.util.BitInput
import java.io.File
import java.io.InputStream
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 29/06/2017.
 */
class RangeFlatChunk(val filePath:String) {
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
    }
}