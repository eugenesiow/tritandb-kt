package com.tritandb.engine.experimental.timestampC

import com.tritandb.engine.tsc.Compressor
import com.tritandb.engine.util.BitOutput

/**
 * TritanDb
 * Created by eugene on 02/06/2017.
 */
class CompressorDeltaAdaptiveRice(timestamp:Long, val out: BitOutput, var columns:Int): Compressor {
    private var oldDelta:Long = -1L
    private var storedTimestamp:Long = timestamp
    private var rleCounter = 1
    private var kRle = 2
    private var kVal = 12
    //    var count = 0
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
//        count++
        compressTimestamp(timestamp)
    }

    /**
     * Closes the block and flushes the remaining byte to OutputStream.
     */
    override fun close() {
        riceEncode(rleCounter.toLong(), kRle)
        riceEncode(oldDelta, kVal)
        kRle = recalculate(rleCounter.toLong(), kRle)
        kVal = recalculate(oldDelta, kVal)
        riceEncode(0, kRle) //encode RLE 0 to end
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
                    riceEncode(rleCounter.toLong(), kRle)
                    riceEncode(oldDelta, kVal)
                    kRle = recalculate(rleCounter.toLong(), kRle)
                    kVal = recalculate(oldDelta, kVal)
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
        val postiveBits = value.ushr(bits)
//        println("${postiveBits}:${value}:${bits}")
        for(i in 1..postiveBits) {
            out.writeBit(true)
        }
        out.writeBit(false)
        val remainder = value and (1L.shl(bits)-1)
        out.writeBits(remainder, bits)
    }

    fun recalculate(u:Long, k:Int): Int {
        val p = u.ushr(k)
        if(p==0L) {
            return k-1
        } else if(p>1) {
            return k+getP(p)
        }
        return k
    }

    fun getP(value:Long):Int {
        var value = value
        var count = 0
        while (value > 0) {
            count++
            value = value.shr(1)
        }
        return count
    }
}