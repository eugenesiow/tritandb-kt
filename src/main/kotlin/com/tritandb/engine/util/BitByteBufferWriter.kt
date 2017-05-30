package com.tritandb.engine.util

import java.io.OutputStream
import java.nio.ByteBuffer
import kotlin.experimental.or

/**
* TritanDb
* Created by eugene on 17/05/2017.
*/
class BitByteBufferWriter(val output: OutputStream): BitOutput {
    val DEFAULT_ALLOCATION = 4096
    val bb: ByteBuffer
    var bitsLeft = java.lang.Byte.SIZE
    var b:Byte

    init{
        bb = ByteBuffer.allocateDirect(DEFAULT_ALLOCATION)
        b = bb.get(bb.position())
    }

    private fun flipByte() {
//        println(b)
        if (bitsLeft == 0) {
            if(!bb.hasRemaining()) {
                writeBB()
            }
            bb.put(b)
            bitsLeft = java.lang.Byte.SIZE
            b = 0
        }
    }

    private fun writeBB() {
        bb.flip()
        val bTemp = ByteArray(bb.remaining())
        bb.get(bTemp)
        output.write(bTemp)
        bb.clear()
        bb.position(bb.remaining())
        bb.flip()
    }

    override fun writeBit(bit: Boolean) {
        if (bit) {
            b = b or (1 shl (bitsLeft - 1)).toByte()
        }
        bitsLeft--
        flipByte()
    }

    override fun writeBits(value: Long, numBits: Int) {
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

    override fun flush() {
        bitsLeft = 0
        flipByte() // Causes write
        writeBB()
    }
}