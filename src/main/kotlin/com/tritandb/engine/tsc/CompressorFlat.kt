package com.tritandb.engine.tsc

import com.tritandb.engine.util.BitOutput

/**
* TritanDb
* Created by eugenesiow on 10/05/2017.
*/
class CompressorFlat(val timestamp:Long, val out: BitOutput, val columns:Int):Compressor {
    private val FIRST_DELTA_BITS = 64
    private var storedLeadingZerosRow:IntArray = IntArray(columns)
    private var storedTrailingZerosRow:IntArray = IntArray(columns)
    private val storedVals:LongArray = LongArray(columns)
    private var storedTimestamp = -1L
    private var storedDelta = 0L
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
            storedVals[i] = values[i]
            out.writeBits(storedVals[i], 64)
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
            val xor = storedVals[i] xor values[i]
            if (xor == 0L) {
                // Write 0
                out.writeBit(false)
            }
            else {
                var leadingZeros = java.lang.Long.numberOfLeadingZeros(xor)
                val trailingZeros = java.lang.Long.numberOfTrailingZeros(xor)
//                if(trailingZeros>=32)
//                    println("${leadingZeros}:${trailingZeros}:${xor}")
                // Check overflow of leading? Can't be 32!
                if (leadingZeros >= 32) {
                    leadingZeros = 31
                }
                // Store bit '1'
                out.writeBit(true)
                if (leadingZeros >= storedLeadingZerosRow[i] && trailingZeros >= storedTrailingZerosRow[i]) {
                    writeExistingLeadingRow(xor, i)
                }
                else {
                    writeNewLeadingRow(xor, leadingZeros, trailingZeros, i)
                }
            }
            storedVals[i] = values[i]
        }
    }
    /**
     * If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
     * store the meaningful XORed value for this column
     *
     * @param xor XOR between previous value and current
     * @param col The column index
     */
    private fun writeExistingLeadingRow(xor:Long, col:Int) {
        out.writeBit(false)
        val significantBits = 64 - storedLeadingZerosRow[col] - storedTrailingZerosRow[col]
        out.writeBits(xor.ushr(storedTrailingZerosRow[col]), significantBits)
    }

    /**
     * store the length of the number of leading zeros in the next 5 bits
     * store length of the meaningful XORed value in the next 6 bits,
     * store the meaningful bits of the XORed value for this column
     * (type b)
     *
     * @param xor XOR between previous value and current
     * @param leadingZeros New leading zeros
     * @param trailingZeros New trailing zeros
     * @param col The column index
     */
    private fun writeNewLeadingRow(xor:Long, leadingZeros:Int, trailingZeros:Int, col:Int) {
        out.writeBit(true)
        out.writeBits(leadingZeros.toLong(), 5) // Number of leading zeros in the next 5 bits
        val significantBits = 64 - leadingZeros - trailingZeros
        out.writeBits(significantBits.toLong().and(0x3F), 6) // Length of meaningful bits in the next 6 bits
        out.writeBits(xor.ushr(trailingZeros), significantBits) // Store the meaningful bits of XOR
        storedLeadingZerosRow[col] = leadingZeros
        storedTrailingZerosRow[col] = trailingZeros
    }

}