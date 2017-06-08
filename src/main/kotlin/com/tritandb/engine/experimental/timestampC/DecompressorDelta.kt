package com.tritandb.engine.experimental.timestampC

import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitReader
import kotlin.coroutines.experimental.buildIterator


/**
 * TritanDb
 * Created by eugene on 25/05/2017.
 */
class DecompressorDelta(val input: BitReader) {
    private var storedTimestamp = 0L
    private var storedDelta = 0
    private var rleCounter = 0
    private var storedVals:LongArray = LongArray(0)
    private var endOfStream = false
    init{
        readHeader()
    }
    private fun readHeader() {
        storedTimestamp = input.readBits(64)
    }
    fun readRows():Iterator<Row> = buildIterator {
        while (!endOfStream) {
            nextRow()
            if (!endOfStream) yield(Row(storedTimestamp, storedVals))
        }
    }
    private fun nextRow() {
        nextTimestamp()
        nextValue()
    }
    private fun nextTimestamp() {
        if(rleCounter==0) {
            rleCounter = readUnsignedLeb128()
//            println("rle:${rleCounter}")
            storedDelta = readUnsignedLeb128()
//            println("storedDelta:${storedDelta}")
        }
        storedTimestamp += storedDelta
        rleCounter--
    }
    private fun nextValue() {
        storedVals = LongArray(0)
    }
    fun readUnsignedLeb128(): Int {
        var result = 0
        var cur: Int
        var count = 0

        do {
            cur = input.readBits(8).toInt() and 0xff
            result = result or (cur and 0x7f shl count * 7)
            count++
        } while (cur and 0x80 == 0x80 && count < 5)

        if (cur and 0x80 == 0x80) {
            endOfStream = true
        }

        return result
    }
}