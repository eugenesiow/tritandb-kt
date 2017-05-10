package com.tritandb.engine.util

import java.io.IOException
import java.io.OutputStream

/**
 * Created by eugene on 10/05/2017.
 */

class BitWriter(val output: OutputStream) {

    var pos = 0
        private set

    private var nextByte = 0
    private var remainingBits = 8

    @Throws(IOException::class)
    fun writeBytes(data: ByteArray) {
        for (b in 0 .. data.size - 1) {
            writeU08(data[b].toInt())
        }
    }

    @Throws(IOException::class)
    fun writeU08(u08: Int) {
        output.write(u08 and 0xff);
        pos++
        if (remainingBits != 8) {
            println("Oh no, there are remaining bits: " + (8 - remainingBits))
            remainingBits = 8
            throw RuntimeException()
        }
    }

    @Throws(IOException::class)
    fun writeU16(u16: Int) {
        writeU08(u16 ushr 8)
        writeU08(u16)
    }

    @Throws(IOException::class)
    fun writeU32(u32: Int) {
        writeU08(u32 ushr 24)
        writeU08(u32 ushr 16)
        writeU08(u32 ushr 8)
        writeU08(u32)
    }

    @Throws(IOException::class)
    fun writeU64(u64: Long) {
        writeU08((u64 ushr 56).toInt())
        writeU08((u64 ushr 48).toInt())
        writeU08((u64 ushr 40).toInt())
        writeU08((u64 ushr 32).toInt())
        writeU08((u64 ushr 24).toInt())
        writeU08((u64 ushr 16).toInt())
        writeU08((u64 ushr 8).toInt())
        writeU08((u64).toInt())
    }

    @Throws(IOException::class)
    fun writeBits(numBits: Int, value: Long) {
        if (numBits > 64 || numBits < 1) {
            throw IllegalArgumentException("Invalid numBits: $numBits, must be in range 1 .. 64")
        }
        if (remainingBits == 8) {
            when (numBits) {
                8 -> return writeU08(value.toInt())
                16 -> return writeU16(value.toInt())
                32 -> return writeU32(value.toInt())
                64 -> return writeU64(value)
            }
        }
        for (i in 1 .. numBits) {
            val mask = 1L shl (numBits - i)
            if (value and mask != 0L) {
                writeBit(1)
            } else {
                writeBit(0)
            }
        }
    }

    @Throws(IOException::class)
    private fun writeBit(bit: Int) {
        nextByte = nextByte shl 1
        nextByte = nextByte or (bit and 1)
        if (--remainingBits == 0) {
            remainingBits = 8
            writeU08(nextByte)
            nextByte = 0
        }
    }

}