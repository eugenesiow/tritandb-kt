package com.tritandb.engine.experimental.valueC

import com.tritandb.engine.tsc.Compressor
import com.tritandb.engine.util.BitOutput

/**
 * TritanDb
 * Created by eugene on 06/06/2017.
 */
class CompressorFpDeltaDelta(timestamp:Long, val out: BitOutput, var columns:Int): Compressor {
    data class DoubleParts(val sign:Boolean, val exponent:Int, val mantissa:Long)
    private val FIRST_DELTA_BITS = 64
    private var storedTimestamp = -1L
    private var storedDelta = 0L
    private var valueDelta = mutableListOf<DoubleParts>()
    private var storedValue = mutableListOf<DoubleParts>()
    private var blockTimestamp:Long = timestamp
//    private var count48 = 0
//    private var count32 = 0
//    private var count58 = 0
    private var count7 = 0
    private var count0 = 0

    init{
        addHeader(timestamp)
    }
    private fun addHeader(timestamp:Long) {
        // One byte: length of the first delta
        // One byte: precision of timestamps
        out.writeBits(columns.toLong(), 32)
        out.writeBits(timestamp, 64)
    }

    /**
     * Adds a new row to the series. Values must be inserted in order.
     *
     * @param timestamp Timestamp in miliseconds
     * @param values LongArray of values for the next row in the series, use java.lang.Double.doubleToRawLongBits function to convert from double to long bits
     */
    override fun addRow(timestamp:Long, values:List<Long>) {
        if (storedTimestamp == -1L) {
            writeFirstRow(timestamp, values)
        }
        else {
            compressTimestamp(timestamp)
            compressValues(values)
        }
    }

    private fun encode(value: Long, i:Int) {
        val (sign, exponent, mantissa) = splitDouble(value)
//        println("raw:${java.lang.Double.longBitsToDouble(value)} exponent:${exponent} mantissa:${mantissa}")
        val newDeltaEx = (exponent - storedValue[i].exponent)
        val newDeltaMt = (mantissa - storedValue[i].mantissa)
        val deltaDEx = newDeltaEx - valueDelta[i].exponent
        val deltaDMt = newDeltaMt - valueDelta[i].mantissa
//        println(deltaDEx)
//        println(deltaDMt)

        out.writeBit(sign) //write the sign bit
        if (deltaDEx == 0)
        {
            out.writeBit(false)
//            count0++
        } else {
            out.writeBit(true)
            out.writeBits((deltaDEx + 2047).toLong(),12)
//            println("deltaDEx ${deltaDEx.toLong()}")
//            count7++
        }

        if (deltaDMt == 0L)
        {
            out.writeBit(false)
//            count0++
        } else if (deltaDMt >= -63 && deltaDMt <= 64)
        {
            out.writeBits(0x02, 2) // store '10'
            out.writeBits(deltaDMt + 63, 7) // Using 7 bits, store the value..
//            count7++
        }
        else if (deltaDMt >= -2147483647 && deltaDMt <= 2147483648)
        {
            out.writeBits(0x06, 3) // store '110'
            out.writeBits(deltaDMt + 2147483647, 32) // Use 32 bits
//            count32++

        } else if (deltaDMt >= -140737488355327 && deltaDMt <= 140737488355328)
        {
            out.writeBits(0x0E, 4) // store '1110'
            out.writeBits(deltaDMt + 140737488355327, 48) // Use 24 bits
//            count48++
        } else
        {
            out.writeBits(0x0F, 4) // Store '1111'
            out.writeBits(deltaDMt + 4503599627370495, 53)
//            count58++
        }
//        println("deltaMt ${deltaDMt}")

        valueDelta[i] = DoubleParts(sign, newDeltaEx, newDeltaMt)
        storedValue[i] = DoubleParts(sign, exponent, mantissa)
    }

    private fun splitDouble(value:Long): DoubleParts {
        return DoubleParts(value.ushr(63) > 0, value.and(0x7ff0000000000000L).ushr(52).toInt(), value.and(0x000fffffffffffffL))
    }

    private fun writeFirstRow(timestamp:Long, values:List<Long>) {
        storedDelta = timestamp - blockTimestamp
        storedTimestamp = timestamp
        out.writeBits(storedDelta, FIRST_DELTA_BITS)
        for (i in 0..columns - 1)
        {
            storedValue.add(splitDouble(values[i]))
            valueDelta.add(splitDouble(values[i]))
//            println("${splitDouble(values[i]).exponent} ${splitDouble(values[i]).mantissa} ${values[i]}:firstrow")
            out.writeBits(values[i], 64)
        }
    }

    /**
     * Closes the block and flushes the remaining byte to OutputStream.
     */
    override fun close() {
        out.writeBits(0x0F, 4)
        out.writeBits(0x7FFFFFFFFFFFFFFF, 64)
        out.writeBit(false) //false
        out.flush()
//        println("${count0} ${count7}")
//        println("${count0} ${count7} ${count32} ${count48} ${count58}")
    }
    /**
     * Stores up to millisecond accuracy, if seconds are used, delta-delta scale automagically
     *
     * @param timestamp epoch
     */
    private fun compressTimestamp(timestamp:Long) {
        // a) Calculate the delta of delta
        val newDelta = (timestamp - storedTimestamp)
        val deltaD = newDelta - storedDelta
//        println(deltaD)
        // If delta is zero, write single 0 bit
        if (deltaD == 0L)
        {
            out.writeBit(false)
        }
//        else if (deltaD >= -31 && deltaD <= 32)
//        {
//            out.writeBits(0x02, 2) // store '10'
//            out.writeBits(deltaD + 31, 6) // Using 7 bits, store the value..
//        }
        else if (deltaD >= -63 && deltaD <= 64)
        {
            out.writeBits(0x02, 2) // store '10'
            out.writeBits(deltaD + 63, 7) // Using 7 bits, store the value..
        }
        else if (deltaD >= -8388607 && deltaD <= 8388608)
        {
            out.writeBits(0x06, 3) // store '1110'
            out.writeBits(deltaD + 8388607, 24) // Use 12 bits
        }
        else if (deltaD >= -2147483647 && deltaD <= 2147483648)
        {
            out.writeBits(0x0E, 4) // store '1110'
            out.writeBits(deltaD + 2147483647, 32) // Use 12 bits
        }
        else
        {
            out.writeBits(0x0F, 4) // Store '1111'
            out.writeBits(deltaD, FIRST_DELTA_BITS) // Store delta using 32 bits
        }
        storedDelta = newDelta
        storedTimestamp = timestamp
    }

    private fun compressValues(values:List<Long>) {
        (0..columns - 1).forEach { i ->
            encode(values[i], i)
        }
    }
}