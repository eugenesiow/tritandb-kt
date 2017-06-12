package com.tritandb.engine.tsc

import com.tritandb.engine.util.BitByteBufferWriter
import org.mapdb.BTreeMap
import java.io.ByteArrayOutputStream
import org.mapdb.DBMaker
import org.mapdb.DB
import org.mapdb.Serializer


/**
 * TritanDb
 * Created by eugene on 12/06/2017.
 */
class CompressorTree(val fileName:String, val columns:Int) {
    data class RowWrite(val value:Long, val bits:Int)

    val o = ByteArrayOutputStream()
    var out = BitByteBufferWriter(o)
    var currentBits = 0
    val MAX_BITS = 4096 * 8
    var rowBits = 0
    private val FIRST_DELTA_BITS = 64
    private var storedLeadingZerosRow:IntArray = IntArray(columns)
    private var storedTrailingZerosRow:IntArray = IntArray(columns)
    private val storedVals:LongArray = LongArray(columns)
    private var storedTimestamp = -1L
    private var storedDelta = 0L
    private var blockTimestamp:Long = 0L
    private val db = DBMaker
            .fileDB(fileName)
            .fileMmapEnable()
            .make()
    private val map = db.treeMap("map")
            .keySerializer(Serializer.LONG_DELTA)
            .valueSerializer(Serializer.BYTE_ARRAY)
            .createOrOpen()
    private val row = mutableListOf<RowWrite>()

    init{
        //setup for rows
        for (i in 0..columns - 1)
        {
            storedLeadingZerosRow[i] = Integer.MAX_VALUE
            storedTrailingZerosRow[i] = 0
        }
        addHeader()
    }
    private fun addHeader() {
        rowWriter(columns.toLong(),32)
    }

    private fun rowWriter(value:Long, bits:Int) {
        row.add(RowWrite(value,bits))
        rowBits += bits
    }

    private fun rowFlush() {
        if(currentBits + rowBits > MAX_BITS) {
            close()
            map.put(blockTimestamp,o.toByteArray())
            currentBits = 0
            o.reset()
            out = BitByteBufferWriter(o)
            addHeader()
            blockTimestamp = storedTimestamp
                    //TODO: RESET everything
            writeRowToOutput()
        } else {
            currentBits += rowBits
        }
        rowBits = 0
    }

    private fun writeRowToOutput() {
        for((value, bits) in row) {
            if(bits >1) {
                out.writeBits(value, bits)
            } else {
                if(value ==0L) out.writeBit(false) else out.writeBit(true)
            }
        }
    }

    /**
     * Adds a new row to the series. Values must be inserted in order.
     *
     * @param timestamp Timestamp in miliseconds
     * @param values LongArray of values for the next row in the series, use java.lang.Double.doubleToRawLongBits function to convert from double to long bits
     */
    fun addRow(timestamp:Long, values:List<Long>) {
        if (storedTimestamp == -1L) {
            writeFirstRow(timestamp, values)
        }
        else {
            compressTimestamp(timestamp)
            compressValues(values)
        }
    }

    private fun writeFirstRow(timestamp:Long, values:List<Long>) {
        blockTimestamp = timestamp
        storedDelta = timestamp - blockTimestamp
        storedTimestamp = timestamp
        rowWriter(storedDelta, FIRST_DELTA_BITS)
        for (i in 0..columns - 1)
        {
            storedVals[i] = values[i]
            rowWriter(storedVals[i], 64)
        }
    }
    /**
     * Closes the block and flushes the remaining byte to OutputStream.
     */
    fun close() {
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
            rowWriter(0,1)
        }
//        else if (deltaD >= -31 && deltaD <= 32)
//        {
//            rowWriter(0x02, 2) // store '10'
//            rowWriter(deltaD + 31, 6) // Using 7 bits, store the value..
//        }
        else if (deltaD >= -63 && deltaD <= 64)
        {
            rowWriter(0x02, 2) // store '10'
            rowWriter(deltaD + 63, 7) // Using 7 bits, store the value..
        }
        else if (deltaD >= -8388607 && deltaD <= 8388608)
        {
            rowWriter(0x06, 3) // store '1110'
            rowWriter(deltaD + 8388607, 24) // Use 12 bits
        }
        else if (deltaD >= -2147483647 && deltaD <= 2147483648)
        {
            rowWriter(0x0E, 4) // store '1110'
            rowWriter(deltaD + 2147483647, 32) // Use 12 bits
        }
        else
        {
            rowWriter(0x0F, 4) // Store '1111'
            rowWriter(deltaD, FIRST_DELTA_BITS) // Store delta using 32 bits
        }
        storedDelta = newDelta
        storedTimestamp = timestamp
    }

    private fun compressValues(values:List<Long>) {
        (0..columns - 1).forEach { i ->
            val xor = storedVals[i] xor values[i]
            if (xor == 0L) {
                // Write 0
                rowWriter(0,1)
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
        rowFlush()
    }
    /**
     * If there at least as many leading zeros and as many trailing zeros as previous value, control bit = 0 (type a)
     * store the meaningful XORed value for this column
     *
     * @param xor XOR between previous value and current
     * @param col The column index
     */
    private fun writeExistingLeadingRow(xor:Long, col:Int) {
        rowWriter(0,1)
        val significantBits = 64 - storedLeadingZerosRow[col] - storedTrailingZerosRow[col]
        rowWriter(xor.ushr(storedTrailingZerosRow[col]), significantBits)
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
        rowWriter(1,1)
        rowWriter(leadingZeros.toLong(), 5) // Number of leading zeros in the next 5 bits
        val significantBits = 64 - leadingZeros - trailingZeros
        rowWriter(significantBits.toLong(), 6) // Length of meaningful bits in the next 6 bits
        rowWriter(xor.ushr(trailingZeros), significantBits) // Store the meaningful bits of XOR
        storedLeadingZerosRow[col] = leadingZeros
        storedTrailingZerosRow[col] = trailingZeros
    }
}