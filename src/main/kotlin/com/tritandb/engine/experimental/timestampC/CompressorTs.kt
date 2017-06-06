package com.tritandb.engine.experimental.timestampC

import com.tritandb.engine.tsc.Compressor
import com.tritandb.engine.util.BitOutput

/**
 * TritanDb
 * Created by eugene on 19/05/2017.
 */
class CompressorTs(timestamp:Long, val out: BitOutput, var columns:Int): Compressor {
    private val FIRST_DELTA_BITS = 64
    private var storedTimestamp = -1L
    private var storedDelta = 0L
    private var blockTimestamp:Long = timestamp
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
        if (storedTimestamp == -1L) {
            writeFirstRow(timestamp, values)
        }
        else {
            compressTimestamp(timestamp)
        }
    }

    private fun writeFirstRow(timestamp:Long, values:List<Long>) {
        storedDelta = timestamp - blockTimestamp
        storedTimestamp = timestamp
        out.writeBits(storedDelta, FIRST_DELTA_BITS)
    }

    /**
     * Closes the block and flushes the remaining byte to OutputStream.
     */
    override fun close() {
        // These are selected to test interoperability and correctness of the solution, this can be read with go-tsz
        out.writeBits(0x0F, 4)
        out.writeBits(0xFFFFFFFF, FIRST_DELTA_BITS)
        out.writeBit(false) //false
        out.flush()
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
        println(deltaD)
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
//        else if (deltaD >= -255 && deltaD <= 256)
//        {
//            out.writeBits(0x06, 3) // store '110'
//            out.writeBits(deltaD + 255, 9) // Use 9 bits
//        }
//        else if (deltaD >= -2047 && deltaD <= 2048)
//        {
//            out.writeBits(0x0E, 4) // store '1110'
//            out.writeBits(deltaD + 2047, 12) // Use 12 bits
//        }

        else
        {
            out.writeBits(0x0F, 4) // Store '1111'
            out.writeBits(deltaD, FIRST_DELTA_BITS) // Store delta using 32 bits
        }
        storedDelta = newDelta
        storedTimestamp = timestamp
    }
}