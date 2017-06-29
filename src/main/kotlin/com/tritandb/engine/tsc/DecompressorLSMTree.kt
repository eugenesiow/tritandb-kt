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

    fun readRange(start:Long,end:Long):Iterator<Row>  = buildIterator {
        val startKey = map.lower(start)!!.key
        val endKey = map.higher(end)!!.key

        map.iterator().forEach {
            if(it.key!! in startKey..endKey)
                yieldAll(DecompressorTreeChunk(it.key, BufferReader(ByteBuffer.wrap(it.value))).readRows())
        }
        map.close()
    }
}