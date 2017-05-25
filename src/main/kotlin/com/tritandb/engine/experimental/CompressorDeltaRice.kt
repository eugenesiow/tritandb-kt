package com.tritandb.engine.experimental

import com.tritandb.engine.tsc.Compressor
import com.tritandb.engine.util.BitOutput

/**
 * TritanDb
 * Created by eugene on 25/05/2017.
 */
class CompressorDeltaRice(timestamp:Long, val out: BitOutput, var columns:Int): Compressor {
    private var oldDelta:Long = -1L
    private var storedTimestamp:Long = timestamp
    private var rleCounter = 1
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
        if(newDelta>=0) {
            if (oldDelta != -1L) {
//            println("${newDelta}:${rleCounter}")
                if (newDelta == oldDelta) {
                    rleCounter++
                } else {
                    riceEncode(rleCounter.toLong(), 2)
                    riceEncode(oldDelta, 16)
                    rleCounter = 1
                }
            }
            storedTimestamp = timestamp
            oldDelta = newDelta
        } else {
            println("${newDelta}:error! out of order series.")
        }
    }

    fun riceEncode(value: Long, bits: Int) {
//        println("${value.ushr(bits)}:${value}")
        for(i in 1..value.ushr(bits))
            out.writeBit(true)
        out.writeBit(false)
        out.writeBits(value, bits)
    }
}