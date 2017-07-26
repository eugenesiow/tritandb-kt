package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BufferReader
import java.io.File
import java.nio.ByteBuffer
import kotlin.coroutines.experimental.buildIterator
import java.io.RandomAccessFile



/**
 * TritanDb
 * Created by eugene on 14/06/2017.
 */
class DecompressorFlatChunk(fileName:String):Decompressor {
    data class IndexEntry(val blockTimestamp:Long, val currentBytes:Int, val count:Int, val values:DoubleArray)

    val idx = File(fileName+".idx").inputStream()
    var file = RandomAccessFile(fileName, "r")
    var endOfIndex = false
    val index = mutableListOf<IndexEntry>()

    init {
        readIndex()
    }

    private fun readIndex() {
        val colB = ByteArray(java.lang.Integer.BYTES)
        idx.read(colB)
        val cols = ByteBuffer.wrap(colB).int
        while (!endOfIndex) {
            val size = java.lang.Double.BYTES + java.lang.Integer.BYTES + java.lang.Integer.BYTES + (cols * java.lang.Long.BYTES)
            val data = ByteArray(size)
            val status = idx.read(data)
            if(status>-1) {
                val bb = ByteBuffer.wrap(data)
                val blockTimestamp = bb.long
                val currentBytes = bb.int
//                if (blockTimestamp == 0x7FFFFFFFFFFFFFFF && currentBytes == 0x7FFFFFFF) {//end of index
//                    endOfIndex = true
//                    break
//                }
                val count = bb.int
                val values: DoubleArray = DoubleArray(cols)
                for (i in 0..cols - 1) {
                    values[i] = toD(bb.long)
                }
                index.add(IndexEntry(blockTimestamp, currentBytes, count, values))
            } else {
                endOfIndex = true
                break
            }
        }
    }

    override fun readRows():Iterator<Row> = buildIterator {
        index.forEach {
            val chunk = ByteArray(it.currentBytes)
//            println(it.currentBytes)
            file.read(chunk)
            val bb = ByteBuffer.wrap(chunk)
            yieldAll(DecompressorTreeChunk(it.blockTimestamp, BufferReader(bb)).readRows())
        }
        file.close()

    }

    fun toD(x:Long):Double {
        return java.lang.Double.longBitsToDouble(x)
    }

    fun readAggr(start:Long,end:Long,col:Int):Iterator<Row>  = buildIterator {
        var startBytes = 0L
        var firstSeek = true
        var firstBlock = -1L
        var firstBlockSize = 0
        var firstBlockT = 0L
        var lastBlock = -1L
        var lastBlockSize = 0
        var lastBlockT = 0L
        var prevCount = 0
        var prevVal = 0.0
        var totalSum = 0.0
        var totalCount = 0L
        for((k,v,count,vals) in index) {
            if(k>start) {
                if(k>end) {
                    totalCount -= prevCount
                    totalSum -= prevVal*prevCount
                    break
                }
                if(firstSeek) {
                    firstBlock = startBytes
                    firstBlockSize = v
                    firstBlockT = k
                    firstSeek = false
                } else {
                    totalCount+=count
                    totalSum += (vals[col]*count)
                    prevCount = count
                    prevVal = vals[col]
                }
                lastBlock = startBytes
                lastBlockSize = v
                lastBlockT = k
            }
            startBytes+=v
        }
        if(firstBlock>0) { //scan start block
            val rF = aggrBlock(firstBlock,firstBlockSize,firstBlockT,start,end,totalSum,totalCount,col)
            totalCount = rF.first
            totalSum = rF.second
            if(firstBlock!=lastBlock) { //scan end block
                val rL = aggrBlock(lastBlock,lastBlockSize,lastBlockT,start,end,totalSum,totalCount,col)
                totalCount = rL.first
                totalSum = rL.second
            }
        }
        yield(Row(0, listOf(java.lang.Double.doubleToLongBits(totalSum/totalCount)).toLongArray()))
        file.close()
    }

    fun aggrBlock(blockOffset:Long,blockSize:Int,ts:Long,start:Long,end:Long,totalSum:Double,totalCount:Long,col:Int):Pair<Long,Double> {
        var sum = totalSum
        var count = totalCount
        file.seek(blockOffset)
        val chunk = ByteArray(blockSize)
        file.read(chunk)
        for((timestamp, values) in DecompressorTreeChunk(ts, BufferReader(ByteBuffer.wrap(chunk))).readRows()) {
            if(timestamp in start..end) {
                sum += values[col]
                count++
            }
        }
        return Pair(count,sum)
    }

    fun readRange(start:Long,end:Long):Iterator<Row>  = buildIterator {
//        var previousBytes = 0L
        var startBytes = 0L
        var firstSeek = true
        for((k,v) in index) {
            if(k>start) {
                if(k>end)
                    break
                if(firstSeek) {
                    file.seek(startBytes)
                    firstSeek = false
                }
                val chunk = ByteArray(v)
                file.read(chunk)
                yieldAll(DecompressorTreeChunk(k, BufferReader(ByteBuffer.wrap(chunk))).readRows())
            }
            startBytes+=v
        }
        file.close()
    }

    fun close() {
        file.close()
        idx.close()
    }
}