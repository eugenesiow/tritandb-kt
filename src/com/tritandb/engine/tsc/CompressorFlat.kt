package com.tritandb.engine.tsc

import com.tritandb.engine.util.BitWriter

/**
 * Created by eugenesiow on 10/05/2017.
 */
class CompressorFlat(timestamp:Long, val out: BitWriter, var columns:Int) {

    private val FIRST_DELTA_BITS:Int = 27
    private var storedLeadingZeros = Integer.MAX_VALUE
    private var storedTrailingZeros = 0
    private var storedLeadingZerosRow:IntArray = IntArray(columns)
    private var storedTrailingZerosRow:IntArray = IntArray(columns)
    private var storedVal:Long = 0
    private val storedVals:LongArray = LongArray(columns)
    private var storedTimestamp:Long = 0
    private var storedDelta:Long = 0
    private var blockTimestamp:Long = timestamp
    init{
        //setup for rows
        for (i in 0..columns - 1)
        {
            storedLeadingZerosRow[i] = Integer.MAX_VALUE
            storedTrailingZerosRow[i] = 0
        }
        addHeader(timestamp)
    }
    private fun addHeader(timestamp:Long) {
        // One byte: length of the first delta
        // One byte: precision of timestamps
        out.writeBits(columns.toLong(), 32)
        out.writeBits(timestamp, 64)
    }
    fun addRow(timestamp:Long, values:LongArray) {
        if (storedTimestamp == 0L)
        {
            writeFirstRow(timestamp, values)
        }
        else
        {
            compressTimestamp(timestamp)
            compressValues(values)
        }
    }
    /**
     * Adds a new long value to the series. Note, values must be inserted in order.
     *
     * @param timestamp Timestamp which is inside the allowed time block (default 24 hours with millisecond precision)
     * @param value next floating point value in the series
     */
    fun addValue(timestamp:Long, value:Long) {
        if (storedTimestamp == 0L)
        {
            writeFirst(timestamp, value)
        }
        else
        {
            compressTimestamp(timestamp)
            compressValue(value)
        }
    }
    /**
     * Adds a new double value to the series. Note, values must be inserted in order.
     *
     * @param timestamp Timestamp which is inside the allowed time block (default 24 hours with millisecond precision)
     * @param value next floating point value in the series
     */
    fun addValue(timestamp:Long, value:Double) {
        if (storedTimestamp == 0L)
        {
            writeFirst(timestamp, java.lang.Double.doubleToRawLongBits(value))
        }
        else
        {
            compressTimestamp(timestamp)
            compressValue(java.lang.Double.doubleToRawLongBits(value))
        }
    }
    private fun writeFirst(timestamp:Long, value:Long) {
        storedDelta = timestamp - blockTimestamp
        storedTimestamp = timestamp
        storedVal = value
        out.writeBits(storedDelta, FIRST_DELTA_BITS)
        out.writeBits(storedVal, 64)
    }
    private fun writeFirstRow(timestamp:Long, values:LongArray) {
        storedDelta = timestamp - blockTimestamp
        storedTimestamp = timestamp
        out.writeBits(storedDelta, FIRST_DELTA_BITS)
        for (i in 0..columns - 1)
        {
            storedVals[i] = values[i]
            out.writeBits(storedVals[i], 64)
        }
    }
    /**
     * Closes the block and writes the remaining stuff to the BitOutput.
     */
    fun close() {
        // These are selected to test interoperability and correctness of the solution, this can be read with go-tsz
        out.writeBits(0x0F, 4)
        out.writeBits(0xFFFFFFFF, 32)
        out.writeBits(0,1) //false
    }
    /**
     * Difference to the original Facebook paper, we store the first delta as 27 bits to allow
     * millisecond accuracy for a one day block.
     *
     * Also, the timestamp delta-delta is not good for millisecond compressions..
     *
     * @param timestamp epoch
     */
    private fun compressTimestamp(timestamp:Long) {
        // a) Calculate the delta of delta
        val newDelta = (timestamp - storedTimestamp)
        val deltaD = newDelta - storedDelta
        // If delta is zero, write single 0 bit
        if (deltaD == 0L)
        {
            out.writeBits(0,1)
        }
        else if (deltaD >= -63 && deltaD <= 64)
        {
            out.writeBits(0x02, 2) // store '10'
            out.writeBits(deltaD, 7) // Using 7 bits, store the value..
        }
        else if (deltaD >= -255 && deltaD <= 256)
        {
            out.writeBits(0x06, 3) // store '110'
            out.writeBits(deltaD, 9) // Use 9 bits
        }
        else if (deltaD >= -2047 && deltaD <= 2048)
        {
            out.writeBits(0x0E, 4) // store '1110'
            out.writeBits(deltaD, 12) // Use 12 bits
        }
        else
        {
            out.writeBits(0x0F, 4) // Store '1111'
            out.writeBits(deltaD, 32) // Store delta using 32 bits
        }
        storedDelta = newDelta
        storedTimestamp = timestamp
    }
    private fun compressValues(values:LongArray) {
        (0..columns - 1).forEach { i ->
            val xor = storedVals[i] xor values[i]
            if (xor == 0L)
            {
                // Write 0
                out.writeBits(0,1)
            }
            else
            {
                var leadingZeros = java.lang.Long.numberOfLeadingZeros(xor)
                val trailingZeros = java.lang.Long.numberOfTrailingZeros(xor)
                // Check overflow of leading? Can't be 32!
                if (leadingZeros >= 32)
                {
                    leadingZeros = 31
                }
                // Store bit '1'
                out.writeBits(0,1)
                if (leadingZeros >= storedLeadingZerosRow[i] && trailingZeros >= storedTrailingZerosRow[i])
                {
                    writeExistingLeadingRow(xor, i)
                }
                else
                {
                    writeNewLeadingRow(xor, leadingZeros, trailingZeros, i)
                }
            }
            storedVals[i] = values[i]
        }
    }
    private fun compressValue(value:Long) {
        // TODO Fix already compiled into a big method
        val xor = storedVal xor value
        if (xor == 0L)
        {
            // Write 0
            out.writeBits(0,1)
        }
        else
        {
            var leadingZeros = java.lang.Long.numberOfLeadingZeros(xor)
            val trailingZeros = java.lang.Long.numberOfTrailingZeros(xor)
            // Check overflow of leading? Can't be 32!
            if (leadingZeros >= 32)
            {
                leadingZeros = 31
            }
            // Store bit '1'
            out.writeBits(0,1)
            if (leadingZeros >= storedLeadingZeros && trailingZeros >= storedTrailingZeros)
            {
                writeExistingLeading(xor)
            }
            else
            {
                writeNewLeading(xor, leadingZeros, trailingZeros)
            }
        }
        storedVal = value
    }
    /**
     * If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
     * store the meaningful XORed value
     *
     * @param xor XOR between previous value and current
     */
    private fun writeExistingLeading(xor:Long) {
        out.writeBits(0,1)
        val significantBits = 64 - storedLeadingZeros - storedTrailingZeros
        out.writeBits(xor.ushr(storedTrailingZeros), significantBits)
    }
    private fun writeExistingLeadingRow(xor:Long, col:Int) {
        out.writeBits(0,1)
        val significantBits = 64 - storedLeadingZerosRow[col] - storedTrailingZerosRow[col]
        out.writeBits(xor.ushr(storedTrailingZerosRow[col]), significantBits)
    }
    /**
     * store the length of the number of leading zeros in the next 5 bits
     * store length of the meaningful XORed value in the next 6 bits,
     * store the meaningful bits of the XORed value
     * (type b)
     *
     * @param xor XOR between previous value and current
     * @param leadingZeros New leading zeros
     * @param trailingZeros New trailing zeros
     */
    private fun writeNewLeading(xor:Long, leadingZeros:Int, trailingZeros:Int) {
        out.writeBits(1,1)
        out.writeBits(leadingZeros.toLong(), 5) // Number of leading zeros in the next 5 bits
        val significantBits = 64 - leadingZeros - trailingZeros
        out.writeBits(significantBits.toLong(), 6) // Length of meaningful bits in the next 6 bits
        out.writeBits(xor.ushr(trailingZeros), significantBits) // Store the meaningful bits of XOR
        storedLeadingZeros = leadingZeros
        storedTrailingZeros = trailingZeros
    }
    private fun writeNewLeadingRow(xor:Long, leadingZeros:Int, trailingZeros:Int, col:Int) {
        out.writeBits(1,1)
        out.writeBits(leadingZeros.toLong(), 5) // Number of leading zeros in the next 5 bits
        val significantBits = 64 - leadingZeros - trailingZeros
        out.writeBits(significantBits.toLong(), 6) // Length of meaningful bits in the next 6 bits
        out.writeBits(xor.ushr(trailingZeros), significantBits) // Store the meaningful bits of XOR
        storedLeadingZerosRow[col] = leadingZeros
        storedTrailingZerosRow[col] = trailingZeros
    }

}