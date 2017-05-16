package main.kotlin.com.tritandb.engine.util

import java.io.OutputStream
import kotlin.experimental.or

/**
 * Created by eugene on 10/05/2017.
 */

class BitWriter(val output: OutputStream) {

    var bitsLeft = java.lang.Byte.SIZE
    var b:Byte = 0

    private fun flipByte() {
        if (bitsLeft == 0) {
            output.write(b.toInt())
            bitsLeft = java.lang.Byte.SIZE
            b = 0
        }
    }

    fun writeBit(bit: Boolean) {
        if (bit) {
            b = b or (1 shl (bitsLeft - 1)).toByte()
        }
        bitsLeft--
        flipByte()
    }

    fun writeBits(value: Long, numBits: Int) {
        var bits = numBits
        while (bits > 0) {
            val bitsToWrite = if (bits > bitsLeft) bitsLeft else bits
            if (bits > bitsLeft) {
                val shift = bits - bitsLeft
                b = b or ((value shr shift) and ((1 shl bitsLeft) - 1).toLong()).toByte()
            } else {
                val shift = bitsLeft - bits
                b = b or (value shl shift).toByte()
            }
            bits -= bitsToWrite
            bitsLeft -= bitsToWrite
            flipByte()
        }
    }

    fun flush() {
        bitsLeft = 0
        flipByte() // Causes write
    }

}