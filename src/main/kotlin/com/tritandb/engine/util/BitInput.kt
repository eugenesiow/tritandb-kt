package com.tritandb.engine.util

/**
 * TritanDb
 * Created by eugene on 07/06/2017.
 */
interface BitInput {
    /**
     * Reads a single bit
     * @return The read Boolean value, false == 0, true == 1
     */
    fun readBit():Boolean

    /**
     * Reads the next number of bits
     * @param bits Number of bits to read
     * @return the value read as a long
     */
    fun readBits(bits: Int): Long

}