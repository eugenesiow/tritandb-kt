package com.tritandb.engine.tsc

import com.indeed.lsmtree.core.StorageType
import com.indeed.lsmtree.core.StoreBuilder
import com.indeed.util.serialization.LongSerializer
import com.indeed.util.serialization.array.ByteArraySerializer
import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BufferReader
import java.io.File
import java.nio.ByteBuffer
import kotlin.coroutines.experimental.buildIterator

/**
 * TritanDb
 * Created by eugene on 23/06/2017.
 */
class DecompressorLSMTree(fileName:String):Decompressor {
    private val map = StoreBuilder(File(fileName), LongSerializer(), ByteArraySerializer()).setCodec(null).setStorageType(StorageType.INLINE).setReadOnly(true).build()

    override fun readRows():Iterator<Row> = buildIterator {
        map.iterator().forEach {
            for (r in DecompressorTreeChunk(it.key, BufferReader(ByteBuffer.wrap(it.value))).readRows())
            yield(r)
        }
        map.close()
    }

    fun close() {
        map.close()
    }

//    fun readRange(start:Long,end:Long):Iterator<Row>  = buildIterator {
//        ////        var previousKey = 0L
//        val keys = sortedSetOf<Long>()
//        var startKey = Long.MAX_VALUE
//        var endKey = 0L
//////        for(k in map.keys) {
//////            if(previousKey==0L)
//////                previousKey = k!!
//////            if (k != null) {
//////                if(k>start)
//////                    startKey = previousKey
//////                else if(k==start)
//////                    startKey = k
//////
//////                if(k>=end)
//////                    endKey = previousKey
//////            }
//////
//////            previousKey = k!!
//////        }
//        for(k in map.keys) {
//            if(k!! in start..end) {
//                if(k<startKey)
//                    startKey = k
//                if(k >endKey)
//                    endKey = k
//            }
//        }
//        if(startKey>endKey) {
//            val tempEnd = endKey
//            endKey = start
//            startKey = tempEnd
//        }
//        map.subMap(startKey,endKey).forEach({
//            yieldAll(DecompressorTreeChunk(it.key, BufferReader(ByteBuffer.wrap(it.value))).readRows())
//        })
//        map.close()
//    }
}