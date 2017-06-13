package com.tritandb.engine.util

import java.nio.ByteBuffer
import kotlin.experimental.or

/**
 * TritanDb
 * Created by eugene on 13/06/2017.
 */
class BufferWriter(val allocation:Int) {
    var totalBits = 0
    val bb = ByteBuffer.allocateDirect(allocation)
    var bitsLeft = java.lang.Byte.SIZE
    var b:Byte

    init{
        b = bb.get(bb.position())
    }

    private fun flipByte() {
        if (bitsLeft == 0) {
//            println("buffer:${bb.remaining()*8}:${bb.capacity()}:${bb.position()*8}:${totalBits/8}")
            if(!bb.hasRemaining()) {
                println("overflow:${bb.remaining()}:${bb.capacity()}:${bb.position()}:${totalBits/8}")
            }
            bb.put(b)
            bitsLeft = java.lang.Byte.SIZE
            b = 0
        }
    }

    fun toByteArray():ByteArray {
        bb.flip()
//        println("pos:${bb.position()},cap:${bb.capacity()},remaining:${bb.remaining()},allocation:$allocation")
        val bTemp = ByteArray(bb.remaining())
        bb.get(bTemp)
        return bTemp
    }

    fun writeBit(bit: Boolean) {
        if (bit) {
            b = b or (1 shl (bitsLeft - 1)).toByte()
        }
        bitsLeft--
        flipByte()
        totalBits++
//        println("numBits:1,value:$bit,buffer")
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
//        println("numBits:$numBits,value:$value,buffer")
        totalBits += numBits
    }

    fun flush() {
        bitsLeft = 0
        flipByte() // Causes write
    }
}