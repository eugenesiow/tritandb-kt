package com.tritandb.engine.query.op

import com.tritandb.engine.tsc.data.Row

/**
 * TritanDb
 * Created by eugene on 19/07/2017.
 */
interface TrOp {
    fun execute()
    var iterator:Iterator<Row>
}