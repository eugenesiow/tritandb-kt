package com.tritandb.engine.query.op

import com.tritandb.engine.tsc.DecompressorFlat
import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitByteBufferReader
import com.tritandb.engine.util.BitInput
import java.io.File
import java.io.InputStream
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 12/06/2017.
 */
class RangeFlat(val filePath:String) {
    fun run(start: Long, end: Long): Iterator<Row> = buildIterator {
        val i: InputStream = File(filePath).inputStream()
        val bi: BitInput = BitByteBufferReader(i)
        val d: DecompressorFlat = DecompressorFlat(bi)
        for (r in d.readRows()) {
            if(r.timestamp>=start) {
                yield(r)
                if(r.timestamp>end) {
                    break
                }
            }
        }
    }
}