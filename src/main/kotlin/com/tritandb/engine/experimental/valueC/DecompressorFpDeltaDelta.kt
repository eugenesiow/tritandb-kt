package com.tritandb.engine.experimental.valueC

import com.tritandb.engine.tsc.Decompressor
import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitInput
import kotlin.coroutines.experimental.buildIterator
import com.tritandb.engine.experimental.valueC.CompressorFpDeltaDelta.DoubleParts

/**
 * TritanDb
 * Created by eugene on 08/06/2017.
 */
class DecompressorFpDeltaDelta(val input: BitInput): Decompressor {
    private val FIRST_DELTA_BITS:Int = 64
    private var storedTimestamp = -1L
    private var storedDelta:Long = 0
    private var storedVals:LongArray = LongArray(0)
    private var columns = 0
    private var blockTimestamp:Long = 0
    private var endOfStream = false
    private var valueDelta = mutableListOf<DoubleParts>()
    private var storedValue = mutableListOf<DoubleParts>()

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
                val startVal = input.readBits(64)
                storedValue.add(splitDouble(startVal))
                valueDelta.add(splitDouble(startVal))
                storedVals[i] = startVal
            }
            storedTimestamp = blockTimestamp + storedDelta
        }
        else {
            nextTimestamp()
            nextValue()
        }
    }
    private fun splitDouble(value:Long): DoubleParts {
        return DoubleParts(value.ushr(63) > 0, value.and(0x7ff0000000000000L).ushr(52).toInt(), value.and(0x000fffffffffffffL))
    }
    private fun joinDouble(doubleParts: DoubleParts): Long {
        val sign = if(doubleParts.sign) 1L else 0L
//        var result = (doubleParts.exponent.toLong().shl(52) + doubleParts.mantissa).and(1L.shl(63).inv())
//        if(doubleParts.sign) {
//            result = (doubleParts.exponent.toLong().shl(52) + doubleParts.mantissa).or(1L.shl(63))
//        }
//        return result
        return (doubleParts.exponent.toLong().shl(52) + doubleParts.mantissa).or(sign.shl(63))
    }
    private fun decode(i:Int):Long {
        val sign = input.readBit()
        val ex = input.readBit()
        var dd = 0
        if(ex) {
            dd = input.readBits(12).toInt() - 2047
        }
        val toRead = manBitsToRead()
        var deltaDelta = input.readBits(toRead)
        when(toRead) {
            7 -> deltaDelta -= 63
            32 -> deltaDelta -=2147483647
            48 -> deltaDelta -=140737488355327
            53 -> deltaDelta -=4503599627370495
        }

//        println("dd:${dd} deltaDelta:${deltaDelta} ${toRead}")
        val deltaE = valueDelta[i].exponent + dd
        val deltaM = valueDelta[i].mantissa + deltaDelta

//        println("orig: ${valueDelta[i].sign} ${valueDelta[i].exponent} ${valueDelta[i].mantissa}")
//        println("delt: ${sign} ${deltaE + storedValue[i].exponent} ${deltaM + storedValue[i].mantissa} ${toRead}")

        valueDelta[i] = DoubleParts(sign, deltaE, deltaM)
        storedValue[i] = DoubleParts(sign, deltaE + storedValue[i].exponent, deltaM + storedValue[i].mantissa)

//        println(joinDouble(storedValue[i]))
//        println(java.lang.Double.longBitsToDouble(joinDouble(storedValue[i])))
        return joinDouble(storedValue[i])
    }
    private fun manBitsToRead():Int {
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
            0x02 -> toRead = 7 // '10'
            0x06 -> toRead = 32 // '110'
            0x0e -> toRead = 48
            0x0F -> toRead = 53
        }
        return toRead
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
            storedVals[i] = decode(i)
        }
    }
}