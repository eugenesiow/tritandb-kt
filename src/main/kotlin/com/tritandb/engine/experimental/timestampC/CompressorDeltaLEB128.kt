package com.tritandb.engine.experimental.timestampC

import com.tritandb.engine.tsc.Compressor
import com.tritandb.engine.util.BitOutput

/**
 * TritanDb
 * Created by eugene on 25/05/2017.
 */
class CompressorDeltaLEB128 (timestamp:Long, val out: BitOutput, var columns:Int): Compressor {
    private var storedTimestamp:Long = timestamp
    init{
        addHeader(timestamp)
    }
    private fun addHeader(timestamp:Long) {
        out.writeBits(timestamp, 64)
    }

    /**
     * Adds a new row to the series. Values must be inserted in order.
     *
     * @param timestamp Timestamp in miliseconds
     * @param values LongArray of values for the next row in the series, use java.lang.Double.doubleToRawLongBits function to convert from double to long bits
     */
    override fun addRow(timestamp:Long, values:List<Long>) {
        compressTimestamp(timestamp)
    }

    /**
     * Closes the block and flushes the remaining byte to OutputStream.
     */
    override fun close() {
        out.writeBits(0x0F, 4)
        out.writeBits(0xFFFFFFFF, 32)
        out.writeBit(false) //false
        out.flush()
    }

    private fun compressTimestamp(timestamp:Long) {
        val newDelta = (timestamp - storedTimestamp)
        writeUnsignedLeb128(newDelta)
        storedTimestamp = timestamp
    }

    fun writeUnsignedLeb128(value: Long) {
        var value = value
        var remaining = value.ushr(7)

        while (remaining != 0L) {
            out.writeBits((value and 0x7f or 0x80),8)
            value = remaining
            remaining = remaining ushr 7
        }

        out.writeBits((value and 0x7f),8)
    }
}