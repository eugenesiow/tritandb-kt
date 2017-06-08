package com.tritandb.engine.util

import java.io.InputStream
import java.nio.ByteBuffer

/**
 * TritanDb
 * Created by eugene on 07/06/2017.
 */
class BitByteBufferReader(val input: InputStream):BitInput {
    val DEFAULT_ALLOCATION = 4096
    val bb: ByteBuffer
    var b:Byte = 0
    var bitsLeft = 0
    var bytesLeft = 0

    init {
        bb = ByteBuffer.allocateDirect(DEFAULT_ALLOCATION)
        flipByte()
    }

    fun flipByte() {
        if(bytesLeft==0) {
//            bb.clear()
            bb.position(0)
            val data = ByteArray(DEFAULT_ALLOCATION)
            input.read(data,0, data.size)
            bb.put(data)
            bytesLeft=bb.position()
            bb.flip()
        }

        if (bitsLeft == 0) {
            val i = bb.get()
            b = i
//            if (i == -1) {
//                throw IOException("Stream was closed")
//            }
            bitsLeft = java.lang.Byte.SIZE
            bytesLeft--
        }
    }

    override fun readBit():Boolean {
        val bit:Int = (b.toInt() shr bitsLeft - 1 and 1)
        bitsLeft--
        flipByte()
        return bit == 1
    }

    override fun readBits(bits: Int): Long {
        var numBits = bits
        var value = 0L
        while(numBits>0) {
            if (numBits > bitsLeft || numBits == java.lang.Byte.SIZE) {
                // Take only the bitsLeft "least significant" bits
                val d = (b.toInt() and (1 shl bitsLeft) - 1)
                value = (value shl bitsLeft) + (d and 0xFF)
                numBits -= bitsLeft
                bitsLeft = 0
            } else {
                // Shift to correct position and take only least significant bits
                value = (value shl numBits) + (b.toInt() ushr (bitsLeft - numBits) and (1 shl numBits) - 1 and 0xFF)
                bitsLeft -= numBits
                numBits = 0
            }
            flipByte()
        }
        return value
    }
}