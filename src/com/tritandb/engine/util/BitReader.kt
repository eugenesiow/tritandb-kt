package com.tritandb.engine.util

import java.io.IOException
import java.io.InputStream

/**
 * Created by eugenesiow on 10/05/2017.
 */
class BitReader(val input: InputStream) {

    var pos = 0
        private set

    private var mark = 0
    private var nextByte = 0
    private var remainingBits = 0

    fun mark(readLimit: Int = 8) {
        mark = pos;
        input.mark(readLimit)
    }

    fun reset() {
        pos = mark;
        input.reset()
    }

    @Throws(IOException::class)
    fun readBytes(len: Int): ByteArray {
        val data = ByteArray(len)
        for (i in 0 .. (len - 1)) {
            data[i] = readU08().toByte()
        }
        return data
    }

    @Throws(IOException::class)
    fun readU08(): Int {
        pos++
        val byte = input.read()
        if (byte == -1) {
            throw IOException("Stream was closed")
        }
        if (remainingBits != 0) {
            println("Oh no, there are remaining bits :(")
            remainingBits = 0
            throw RuntimeException()
        }
        return byte
    }

    @Throws(IOException::class)
    fun readU16(): Int {
        return (readU08() shl 8) or readU08()
    }

    @Throws(IOException::class)
    fun readU32(): Int {
        return (readU08() shl 24) or (readU08() shl 16) or (readU08() shl 8) or readU08()
    }

    @Throws(IOException::class)
    fun readU64(): Long {
        return (readU08().toLong() shl 56) or (readU08().toLong() shl 48) or
                (readU08().toLong() shl 40) or (readU08().toLong() shl 32) or
                (readU08().toLong() shl 24) or (readU08().toLong() shl 16) or
                (readU08().toLong() shl 8)  or  readU08().toLong()
    }

    @Throws(IOException::class)
    fun readBits(numBits: Int): Long {
        if (numBits > 64 || numBits < 1) {
            throw IllegalArgumentException("Invalid numBits: $numBits, must be in range 1 .. 64")
        }
        if (remainingBits == 0) {
            when (numBits) {
                8 -> return readU08().toLong()
                16 -> return readU16().toLong()
                32 -> return readU32().toLong()
                64 -> return readU64().toLong()
            }
        }
        var bits: Long = 0
        for (i in 1 .. numBits) {
            bits = (bits shl 1) or nextBit().toLong()
        }
        return bits
    }

    @Throws(IOException::class)
    private fun nextBit(): Int {
        if (remainingBits == 0) {
            nextByte = readU08()
            remainingBits = 7
        } else {
            remainingBits--
        }
        val bit = (nextByte and 0x80) shr 7
        nextByte = nextByte shl 1
        return bit
    }

}