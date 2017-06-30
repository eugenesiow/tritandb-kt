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
    data class IndexEntry(val blockTimestamp:Long, val currentBytes:Int)

    val idx = File(fileName+".idx").inputStream()
    var file = RandomAccessFile(fileName, "r")
    var endOfIndex = false
    val index = mutableListOf<IndexEntry>()

    init {
        readIndex()
    }

    private fun readIndex() {
        while (!endOfIndex) {
            val data = ByteArray(java.lang.Double.BYTES + java.lang.Integer.BYTES)
            idx.read(data)
            val bb = ByteBuffer.wrap(data)
            val blockTimestamp = bb.long
            val currentBytes = bb.int
            if (blockTimestamp == 0x7FFFFFFFFFFFFFFF && currentBytes == 0x7FFFFFFF) {//end of index
                endOfIndex = true
                break
            }
            index.add(IndexEntry(blockTimestamp,currentBytes))
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

    fun readRange(start:Long,end:Long):Iterator<Row>  = buildIterator {
        var previousBytes = 0L
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
}