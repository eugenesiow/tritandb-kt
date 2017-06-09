package com.tritandb.engine.experimental.valueC

import com.tritandb.engine.tsc.Decompressor
import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitInput
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 22/05/2017.
 */
class DecompressorFpc(val input: BitInput):Decompressor {
    private val FIRST_DELTA_BITS:Int = 64
    private var storedTimestamp = -1L
    private var storedDelta:Long = 0
    private var storedVals:LongArray = LongArray(0)
    private var columns = 0
    private var blockTimestamp:Long = 0
    private var endOfStream = false

    private val logOfTableSize = 16
    private var predictor1 = FcmPredictor(logOfTableSize)
    private var predictor2 = DfcmPredictor(logOfTableSize)

    init{
        readHeader()
    }
    private fun readHeader() {
        columns = input.readBits(32).toInt()
        storedVals = LongArray(columns)
        blockTimestamp = input.readBits(64)
        if(columns==0) {
            endOfStream = true
        }
    }
    override fun readRows():Iterator<Row> = buildIterator {
        while (!endOfStream) {
            nextRow()
            if (!endOfStream) yield(Row(storedTimestamp, storedVals))
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
                storedVals[i] = decode()
            }
            storedTimestamp = blockTimestamp + storedDelta
        }
        else {
            nextTimestamp()
            nextValue()
        }
    }
    private fun decode():Long {
        val header = input.readBits(4).toInt()

        var prediction: Long
        if (header and 0x08 > 0) {
            prediction = predictor2.prediction
        } else {
            prediction = predictor1.prediction
        }

        var numZeroBytes = header and 0x07
        if (numZeroBytes > 3) {
            numZeroBytes++
        }
        var diff = input.readBits((8 - numZeroBytes)*8)
        var actual = prediction xor diff

        predictor1.update(actual)
        predictor2.update(actual)

//        println("${actual}:${(8 - numZeroBytes)*8}:${header}")

        return actual
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
            storedVals[i] = decode()
        }
    }
}