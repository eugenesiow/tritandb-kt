package com.tritandb.engine.util

/**
* TritanDb
* Created by eugene on 17/05/2017.
*/
interface BitOutput {
    /**
     * Stores a single bit, 1 is true, 0 is false.
     * @param bit Boolean, 0 or 1 bit to write false == 0, true == 1
     */
    fun writeBit(bit: Boolean)

    /**
     * Write the given long value using the defined amount of least significant bits.
     * @param value value to be written in a Long
     * @param numBits Number of bits to write
     */
    fun writeBits(value: Long, numBits: Int)

    /**
     * Flushes the current byte to the underlying data structure or file
     */
    fun flush()
}