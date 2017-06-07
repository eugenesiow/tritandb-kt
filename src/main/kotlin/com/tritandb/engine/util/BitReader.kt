package com.tritandb.engine.util

import java.io.IOException
import java.io.InputStream
import java.nio.ByteBuffer

/**
* TritanDb
* Created by eugenesiow on 10/05/2017.
*/
class BitReader(val input: InputStream):BitInput {

    var b = 0
    var bitsLeft = 0

    init {
        flipByte()
    }
    /**
     * Reads the next bit and returns a boolean representing it.

     * @return true if the next bit is 1, otherwise 0.
     */
    override fun readBit(): Boolean {
        val bit = (b shr bitsLeft - 1 and 1)
        bitsLeft--
        flipByte()
        return bit == 1
    }

    /**
     * Reads a long from the next X bits that represent the least significant bits in the long value.

     * @param bits How many next bits are read from the stream
     * *
     * @return long value that was read from the stream
     */
    override fun readBits(bits: Int): Long {
        var numBits = bits
        var value = 0L
        while(numBits>0) {
            if (numBits > bitsLeft || numBits == java.lang.Byte.SIZE) {
                // Take only the bitsLeft "least significant" bits
                val d = (b and (1 shl bitsLeft) - 1)
                value = (value shl bitsLeft) + (d and 0xFF)
                numBits -= bitsLeft
                bitsLeft = 0
            } else {
                // Shift to correct position and take only least significant bits
                value = (value shl numBits) + (b ushr (bitsLeft - numBits) and (1 shl numBits) - 1 and 0xFF)
                bitsLeft -= numBits
                numBits = 0
            }
            flipByte()
        }
        return value
    }

    private fun flipByte() {
        if (bitsLeft == 0) {
            val i = input.read()
            b = i
            if (i == -1) {
                throw IOException("Stream was closed")
            }

            bitsLeft = java.lang.Byte.SIZE
        }
    }

}