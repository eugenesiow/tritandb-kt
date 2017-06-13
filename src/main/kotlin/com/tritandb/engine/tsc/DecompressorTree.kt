package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.Row
import com.tritandb.engine.util.BufferReader
import org.mapdb.DBMaker
import org.mapdb.Serializer
import java.nio.ByteBuffer
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
//    private val map = db.hashMap("map")
//            .keySerializer(Serializer.LONG_DELTA)
//            .valueSerializer(Serializer.BYTE_ARRAY)
//            .createOrOpen()
    private val map = db.treeMap("map")
            .keySerializer(Serializer.LONG_DELTA)
            .valueSerializer(Serializer.BYTE_ARRAY)
            .createOrOpen()

    override fun readRows():Iterator<Row> = buildIterator {
        map.forEach({
            for(r in DecompressorTreeChunk(it.key, BufferReader(ByteBuffer.wrap(it.value))).readRows())
                yield(r)
        })
        map.close()
    }

    fun readRange(start:Long,end:Long):Iterator<Row>  = buildIterator {
        var previousKey = 0L
        var startKey = 0L
        var endKey = 0L
        for(k in map.keys) {
            if(previousKey==0L)
                previousKey = k!!
            if (k != null) {
                if(k>start)
                    startKey = previousKey
                else if(k==start)
                    startKey = k

                if(k>=end)
                    endKey = previousKey
            }

            previousKey = k!!
        }
        map.subMap(startKey,endKey).forEach({
            for(r in DecompressorTreeChunk(it.key, BufferReader(ByteBuffer.wrap(it.value))).readRows())
                yield(r)
        })
        map.close()
    }
}