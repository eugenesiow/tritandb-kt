package com.tritandb.engine.experimental.timestampC

import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BitReader
import java.io.IOException
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 26/05/2017.
 */
class DecompressorDeltaRice(val input: BitReader) {
    private var storedTimestamp = 0L
    private var storedDelta = 0
    private var rleCounter = 0
    private var storedVals:LongArray = LongArray(0)
    private var endOfStream = false
    init{
        readHeader()
    }
    private fun readHeader() {
        storedTimestamp = input.readBits(64)
    }
    fun readRows():Iterator<Row> = buildIterator {
        while(!endOfStream) {
            nextRow()
            if (!endOfStream) yield(Row(storedTimestamp, storedVals))
        }
    }
    private fun nextRow() {
        nextTimestamp()
        nextValue()
    }
    private fun nextTimestamp() {
        if(rleCounter==0) {
            rleCounter = readRice(2)
            if(rleCounter==0) {
                endOfStream = true
                return //need to take into account nextValue
            }
//            println("rle:${rleCounter}")
            storedDelta = readRice(12)
//            println("storedDelta:${storedDelta}")
        }
        storedTimestamp += storedDelta
        rleCounter--
    }
    private fun nextValue() {
        storedVals = LongArray(0)
    }
    private fun readRice(bits:Int):Int {
        var bit = try {input.readBit()} catch (e: IOException) {endOfStream = true}
        var count = 0
        while(bit==true) {
            count++
            bit = input.readBit()
            if(count>=Integer.MAX_VALUE) {
                endOfStream = true
                break
            }
        }
//        var remainder = 0
//        try { remainder = input.readBits(bits).toInt() } catch (e: IOException) {endOfStream = true}
        return (count).shl(bits) + input.readBits(bits).toInt()
    }

}