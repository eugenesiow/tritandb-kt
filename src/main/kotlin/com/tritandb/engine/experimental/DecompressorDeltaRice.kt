package com.tritandb.engine.experimental

import com.tritandb.engine.util.BitReader
import main.kotlin.com.tritandb.engine.tsc.data.Row
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
//            println("rle:${rleCounter}")
            storedDelta = readRice(16)
        }
        storedTimestamp += storedDelta
        rleCounter--
    }
    private fun nextValue() {
        storedVals = LongArray(0)
    }
    private fun readRice(bits:Int):Int {
        var bit = input.readBit()
        var count = 1
        while(bit!=false) {
//            println("loop")
            count++
            bit = input.readBit()
            if(count>=Integer.MAX_VALUE) {
                endOfStream = true
                break
            }
        }
//        println("${count}")
        return (count-1).shl(bits) + input.readBits(bits).toInt()
    }

}