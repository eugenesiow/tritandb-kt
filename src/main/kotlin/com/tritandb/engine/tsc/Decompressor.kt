package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.Row

/**
 * TritanDb
 * Created by eugene on 07/06/2017.
 */
interface Decompressor {
    fun readRows():Iterator<Row>
}