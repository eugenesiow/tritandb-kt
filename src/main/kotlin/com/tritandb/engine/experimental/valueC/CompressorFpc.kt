package com.tritandb.engine.experimental.valueC

import com.tritandb.engine.tsc.Compressor
import com.tritandb.engine.util.BitOutput
import kotlin.experimental.or

/**
 * TritanDb
 * Created by eugene on 22/05/2017.
 */
class CompressorFpc(timestamp:Long, val out: BitOutput, var columns:Int): Compressor {
    private val FIRST_DELTA_BITS = 64
    private var storedTimestamp = -1L
    private var storedDelta = 0L
    private var blockTimestamp:Long = timestamp
    private val logOfTableSize = 16
    internal val predictor1 = FcmPredictor(logOfTableSize)
    internal val predictor2 = DfcmPredictor(logOfTableSize)
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

    private fun writeFirstRow(timestamp:Long, values:List<Long>) {
        storedDelta = timestamp - blockTimestamp
        storedTimestamp = timestamp
        out.writeBits(storedDelta, FIRST_DELTA_BITS)
        for (i in 0..columns - 1)
        {
            encode(values[i])
        }
    }

    private fun encodeZeroBytes(diff1: Long): Int {
        var leadingZeroBytes = java.lang.Long.numberOfLeadingZeros(diff1) / 8
        if (leadingZeroBytes >= 4) {
            leadingZeroBytes--
        }
        return leadingZeroBytes
    }

    private fun  encode(value: Long) {
        val bits = value
        val diff1 = predictor1.prediction xor bits
        val diff2 = predictor2.prediction xor bits

        val predictor1Better = java.lang.Long.numberOfLeadingZeros(diff1) >= java.lang.Long.numberOfLeadingZeros(diff2)

        predictor1.update(bits)
        predictor2.update(bits)

        var code: Byte = 0
        if (predictor1Better) {
            val zeroBytes = encodeZeroBytes(diff1)
            code = code or zeroBytes.toByte()
        } else {
            code = code or 0x08
            val zeroBytes = encodeZeroBytes(diff2)
            code = code or zeroBytes.toByte()
        }
        out.writeBits(code.toLong(),4)

        if (predictor1Better) {
            out.writeBits(diff1, bitCounter(diff1))
//            println("${predictor1Better}:${diff1}:${bitCounter(diff1)}:${code}")
        } else {
            out.writeBits(diff2, bitCounter(diff2))
//            println("${predictor1Better}:${diff2}:${bitCounter(diff2)}:${code}")
        }
    }

    private fun bitCounter(diff: Long): Int {
        var encodedZeroBytes = encodeZeroBytes(diff)
        if (encodedZeroBytes > 3) {
            encodedZeroBytes++
        }
        return ((8-encodedZeroBytes)*8)
    }

    /**
     * Closes the block and flushes the remaining byte to OutputStream.
     */
    override fun close() {
        out.writeBits(0x0F, 4)
        out.writeBits(0x7FFFFFFFFFFFFFFF, 64)
        out.writeBit(false) //false
        out.flush()
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
           encode(values[i])
        }
    }
}