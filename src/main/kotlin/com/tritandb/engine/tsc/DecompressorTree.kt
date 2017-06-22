package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BufferReader
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.mapdb.DBMaker
import org.mapdb.Serializer
import java.nio.ByteBuffer
import kotlin.Long.Companion.MAX_VALUE
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 13/06/2017.
 */
class DecompressorTree(fileName:String):Decompressor {
    private val db = DBMaker
            .fileDB(fileName)
            .fileMmapEnable()
            .make()
    private val map = db.treeMap("map")
            .keySerializer(Serializer.LONG)
            .valueSerializer(Serializer.BYTE_ARRAY)
            .createOrOpen()

    override fun readRows():Iterator<Row> = buildIterator {
        map.forEach({
            for (r in DecompressorTreeChunk(it.key, BufferReader(ByteBuffer.wrap(it.value))).readRows())
                yield(r)
        })
        map.close()
    }

    fun readRowsPar():Iterator<Row> = buildIterator {
        val jobs = arrayListOf<Job>()
        map.forEach({
            jobs += launch(CommonPool) {
                for (r in DecompressorTreeChunk(it.key, BufferReader(ByteBuffer.wrap(it.value))).readRows())
                    yield(r)
            }
        })
        runBlocking {
            jobs.forEach {
                it.join()
            }
        }
        map.close()
    }

    fun close() {
        map.close()
    }

    fun readRange(start:Long,end:Long):Iterator<Row>  = buildIterator {
////        var previousKey = 0L
        val keys = sortedSetOf<Long>()
        var startKey = MAX_VALUE
        var endKey = 0L
////        for(k in map.keys) {
////            if(previousKey==0L)
////                previousKey = k!!
////            if (k != null) {
////                if(k>start)
////                    startKey = previousKey
////                else if(k==start)
////                    startKey = k
////
////                if(k>=end)
////                    endKey = previousKey
////            }
////
////            previousKey = k!!
////        }
        for(k in map.keys) {
            if(k!! in start..end) {
                if(k<startKey)
                    startKey = k
                if(k >endKey)
                    endKey = k
            }
        }
        if(startKey>endKey) {
            val tempEnd = endKey
            endKey = start
            startKey = tempEnd
        }
        map.subMap(startKey,endKey).forEach({
            yieldAll(DecompressorTreeChunk(it.key, BufferReader(ByteBuffer.wrap(it.value))).readRows())
        })
        map.close()
    }
}