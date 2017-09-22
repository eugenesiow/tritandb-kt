package com.tritandb.engine.query.op

import com.tritandb.engine.tsc.data.Row
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 22/09/2017.
 */
class Distinct {
    fun distinct(rows:Iterator<Row>):Iterator<Row> {
        return buildIterator {
            var previousRow = Row(0, listOf<Long>().toLongArray())
            for (row in rows) {
                if (row.timestamp != previousRow.timestamp || row.values != previousRow.values)
                    yield(row)
                previousRow = row
            }
        }
    }
}