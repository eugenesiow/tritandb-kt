package com.tritandb.engine.experimental.timestampC

import com.tritandb.engine.tsc.data.Row
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 02/06/2017.
 */
class DecompressorTs(val input: com.tritandb.engine.util.BitReader) {
    private val FIRST_DELTA_BITS:Int = 64
    private var storedTimestamp = -1L
    private var storedDelta:Long = 0
    private var blockTimestamp:Long = 0
    private var endOfStream = false
    private var storedVals:LongArray = LongArray(0)
    init{
        readHeader()
    }
    private fun readHeader() {
        blockTimestamp = input.readBits(64)
    }

    fun readRows():Iterator<Row> = buildIterator {
        while (!endOfStream) {
            nextRow()
            if (!endOfStream) yield(Row(storedTimestamp, storedVals))
        }
    }
    private fun nextRow() {
        if (storedTimestamp == -1L) {
            // First item to read
            storedDelta = input.readBits(FIRST_DELTA_BITS)
//            if (storedDelta == ((1 shl FIRST_DELTA_BITS) - 1).toLong()) {
//                endOfStream = true
//                return
//            }
            storedTimestamp = blockTimestamp + storedDelta
        }
        else {
            nextTimestamp()
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
//            println(deltaDelta)
            if (toRead == FIRST_DELTA_BITS) {
                if (deltaDelta == 0xFFFFFFFFFFFFFFFL) {
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
}