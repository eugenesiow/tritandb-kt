package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitInput
import java.io.IOException
import kotlin.coroutines.experimental.buildIterator

/**
* TritanDb
* Created by eugene on 11/05/2017.
*/
class DecompressorFlat(val input: BitInput):Decompressor {
    private val FIRST_DELTA_BITS:Int = 64
    private var storedLeadingZerosRow:IntArray = IntArray(1)
    private var storedTrailingZerosRow:IntArray = IntArray(1)
    private var storedVals:LongArray = LongArray(1)
    private var storedTimestamp = -1L
    private var storedDelta:Long = 0
    private var columns = 0
    private var blockTimestamp:Long = 0
    private var endOfStream = false
    init{
        readHeader()
    }
    private fun readHeader() {
        columns = input.readBits(32).toInt()
        storedLeadingZerosRow = IntArray(columns)
        storedTrailingZerosRow = IntArray(columns)
        storedVals = LongArray(columns)
        for (i in 0..columns - 1) {
            storedLeadingZerosRow[i] = Integer.MAX_VALUE
            storedTrailingZerosRow[i] = 0
        }
        blockTimestamp = input.readBits(64)
    }
//    fun readRow(): Row? {
//        next()
//        if (endOfStream) {
//            return null
//        }
//        return Row(storedTimestamp, storedVals)
//    }
//    override fun next():Row {
//        return Row(storedTimestamp, storedVals)
//    }
//    override fun hasNext():Boolean {
//        nextRow()
//        return !endOfStream
//    }
    override fun readRows():Iterator<Row> = buildIterator {
        while(!endOfStream) {
            try {
                nextRow()
            } catch(e: IOException) {
                println(storedTimestamp)
            }
            if (!endOfStream) yield(Row(storedTimestamp, storedVals))
        }
    }
    private fun nextRow() {
        if (storedTimestamp == -1L) {
            // First item to read
            storedDelta = input.readBits(FIRST_DELTA_BITS)
            if (storedDelta == ((1 shl 27) - 1).toLong()) {
                endOfStream = true
                return
            }
            for (i in 0..columns - 1) {
                storedVals[i] = input.readBits(64)
            }
            storedTimestamp = blockTimestamp + storedDelta
        }
        else {
            nextTimestamp()
            nextValue()
        }
    }
    private fun bitsToRead():Int {
        var value = 0x00
        for (i in 0..3) {
            value = value shl 1
            val bit = input.readBit()
            if (bit) {
                value = value or 0x01
            }
            else {
                break
            }
        }
        var toRead = 0
        when (value) {
            0x00 -> {}
//            0x02 -> toRead = 6 // '10'
            0x02 -> toRead = 7 // '10'
//            0x06 -> toRead = 9 // '110'
//            0x0e -> toRead = 12
            0x06 -> toRead = 24 // '110'
            0x0e -> toRead = 32
            0x0F -> toRead = FIRST_DELTA_BITS
        }
        return toRead
    }
    private fun nextTimestamp() {
        // Next, read timestamp
        var deltaDelta:Long = 0
        val toRead = bitsToRead()
        if (toRead > 0) {
            deltaDelta = input.readBits(toRead)
            if (toRead == FIRST_DELTA_BITS) {
                if (deltaDelta == 0xFFFFFFFFFFFFFFF) {
                    // End of stream
                    endOfStream = true
                    return
                }
            }
            else {
                // Turn deltaDelta long value back to signed one
                when(toRead) {
//                    6 -> deltaDelta -=31
                    7 -> deltaDelta -= 63
//                    9 -> deltaDelta -=255
//                    12 -> deltaDelta -=2047
                    24 -> deltaDelta -=8388607
                    32 -> deltaDelta -=2147483647
                }
            }
        }
        storedDelta += deltaDelta
        storedTimestamp += storedDelta
    }
    private fun nextValue() {
        for (i in 0..columns - 1) {
            // Read value
            if (input.readBit()) {
                // else -> same value as before
                if (input.readBit()) {
                    // New leading and trailing zeros
                    storedLeadingZerosRow[i] = input.readBits(5).toInt()
                    var significantBits = input.readBits(6).toInt()
                    if (significantBits == 0) {
                        significantBits = 64
                    }
                    storedTrailingZerosRow[i] = 64 - significantBits - storedLeadingZerosRow[i]
                }
                var value = input.readBits(64 - storedLeadingZerosRow[i] - storedTrailingZerosRow[i])
                value = value shl storedTrailingZerosRow[i]
                value = storedVals[i] xor value
                storedVals[i] = value
            }
        }
    }
}