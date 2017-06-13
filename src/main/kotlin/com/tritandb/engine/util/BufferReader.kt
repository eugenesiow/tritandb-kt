package com.tritandb.engine.util

import java.io.IOException
import java.nio.ByteBuffer

/**
 * TritanDb
 * Created by eugene on 13/06/2017.
 */
class BufferReader(val input: ByteBuffer) {
    var b:Byte = 0
    var bitsLeft = 0

    init {
        flipByte()
    }

    fun flipByte() {
        if (bitsLeft == 0) {
            if(!input.hasRemaining())
                throw IOException("Stream Closed")
            val i = input.get()
            b = i
//            if (i == -1) {
//                throw IOException("Stream was closed")
//            }
            bitsLeft = java.lang.Byte.SIZE
        }
    }

    fun readBit():Boolean {
        val bit:Int = (b.toInt() shr bitsLeft - 1 and 1)
        bitsLeft--
//        if(bitsLeft == 0 && input.remaining()==0) return true
        flipByte()
        return bit == 1
    }

//    fun isEmpty():Boolean {
//        return input.remaining()==0
//    }

    fun readBits(bits: Int): Long {
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
//            if(bitsLeft == 0 && input.remaining()==0) return 0x7FFFFFFFFFFFFFFF
            flipByte()
        }
        return value
    }
}