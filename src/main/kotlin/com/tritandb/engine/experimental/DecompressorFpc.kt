package com.tritandb.engine.experimental

import com.tritandb.engine.util.BitReader
import main.kotlin.com.tritandb.engine.tsc.data.Row
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 22/05/2017.
 */
class DecompressorFpc(val input: BitReader) {
    private val FIRST_DELTA_BITS:Int = 27
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
    }
    fun readRows():Iterator<Row> = buildIterator {
        while(!endOfStream) {
            nextRow()
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
        if (header and 0x80 != 0) {
            prediction = predictor2.getPrediction()
        } else {
            prediction = predictor1.getPrediction()
        }

        var numZeroBytes = header and 0x70 shr 4
        if (numZeroBytes > 3) {
            numZeroBytes++
        }
        var diff = input.readBits((8 - numZeroBytes)*8)
        var actual = prediction xor diff

        predictor1.update(actual)
        predictor2.update(actual)

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
            0x06 -> toRead = 9 // '110'
            0x0e -> toRead = 12
            0x0F -> toRead = 32
        }
        return toRead
    }
    private fun nextTimestamp() {
        // Next, read timestamp
        var deltaDelta:Long = 0
        val toRead = bitsToRead()
        if (toRead > 0) {
            deltaDelta = input.readBits(toRead)
            if (toRead == 32) {
                if (deltaDelta.toInt() == 0xFFFFFFFF.toInt()) {
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
                    9 -> deltaDelta -=255
                    12 -> deltaDelta -=2047
                }
            }
            deltaDelta = deltaDelta.toInt().toLong()
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