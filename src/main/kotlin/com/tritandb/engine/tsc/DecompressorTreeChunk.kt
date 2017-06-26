package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitInput
import java.io.IOException
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 13/06/2017.
 */
class DecompressorTreeChunk(val blockTimestamp:Long, val input:BitInput):Decompressor {
    private val FIRST_DELTA_BITS:Int = 64
    private var storedLeadingZerosRow:IntArray = IntArray(1)
    private var storedTrailingZerosRow:IntArray = IntArray(1)
    private var storedVals:LongArray = LongArray(1)
    private var storedTimestamp = -1L
    private var storedDelta = 0L
    private var columns = 0
    private var endOfStream = false

    init {
        setupHeader()
    }

    override fun readRows(): Iterator<Row> = buildIterator {
        while(!endOfStream) {
            try {
                nextRow()
            } catch(e: IOException) {
                endOfStream = true
//                    println("${storedTimestamp} ${storedDelta}")
            }
            if (!endOfStream) yield(Row(storedTimestamp, storedVals))
//            else yield(Row(storedTimestamp, listOf<Long>(0,0,0,0,0,0).toLongArray()))
        }
//            println("ended")
    }

    private fun setupHeader() {
        columns = input.readBits(32).toInt()
//        println(columns)
        storedLeadingZerosRow = IntArray(columns)
        storedTrailingZerosRow = IntArray(columns)
        storedVals = LongArray(columns)
        for (i in 0..columns - 1) {
            storedLeadingZerosRow[i] = Integer.MAX_VALUE
            storedTrailingZerosRow[i] = 0
        }
        if(columns==0) {
            endOfStream = true
        }
    }
    private fun nextRow() {
        if (storedTimestamp == -1L) {
            // First item to read
            storedDelta = input.readBits(FIRST_DELTA_BITS)
            if (storedDelta.ushr(FIRST_DELTA_BITS-4).toInt()==0x0F && storedDelta.shl(4).ushr(4) == 0x07FFFFFFFFFFFFFF) {
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
        var deltaDelta = 0L
//        if(input.isEmpty()) {
//            endOfStream = true
//            return
//        }
        val toRead = bitsToRead()
        if (toRead > 0) {
            deltaDelta = input.readBits(toRead)
            if (toRead == FIRST_DELTA_BITS) {
                if (deltaDelta == 0x7FFFFFFFFFFFFFFF) {
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