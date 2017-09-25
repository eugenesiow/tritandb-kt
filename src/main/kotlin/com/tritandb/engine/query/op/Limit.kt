package com.tritandb.engine.query.op

import com.tritandb.engine.tsc.data.Row
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 22/09/2017.
 */
class Limit(val rows:Iterator<Row>, private val limit:Int, private val offset:Int):TrOp {
    override fun execute():Iterator<Row> {
        return buildIterator {
            var totalIdx = 0
            var resultIdx = 0
            for (row in rows) {
                if(totalIdx>=offset) {
                    resultIdx++
                    yield(row)
                }
                if(resultIdx>=limit)
                    break
                totalIdx++
            }
        }
    }
}