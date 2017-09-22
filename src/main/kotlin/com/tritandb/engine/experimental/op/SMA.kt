package com.tritandb.engine.experimental.op

import com.tritandb.engine.tsc.data.Row
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 22/09/2017.
 */
class SMA {
    fun sma(rows:Iterator<Row>):Iterator<Row> {
        return buildIterator {
            for (row in rows) {
                yield(row)
            }
        }
    }
}