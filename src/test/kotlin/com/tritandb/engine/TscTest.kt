package com.tritandb.engine

import com.nhaarman.mockito_kotlin.*
import com.tritandb.engine.experimental.CompressorFpc
import com.tritandb.engine.experimental.DecompressorFpc
import com.tritandb.engine.tsc.CompressorFlat
import com.tritandb.engine.tsc.DecompressorFlat
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitReader
import org.junit.Test
import java.io.*
import kotlin.test.assertEquals

/**
 * TritanDb
 * Created by eugene on 18/05/2017.
 */

class CompressorTest {
    @Test fun compressRow_writeBytes() {
        val b: BitByteBufferWriter = mock()
//        whenever(b.writeBits(any(),any())).then { println(it) }
//        whenever(b.writeBit(any())).then { println(it) }
        val timestamp = 0L
        val c = CompressorFlat(timestamp, b, 2)
        c.addRow(timestamp, listOf(0,1))
        c.addRow(timestamp+1, listOf(1,2))
        c.close()

        verify(b,times(15)).writeBits(any(),any())
        verify(b,times(5)).writeBit(any())

        inOrder(b) {
            verify(b).writeBits(2L, 32) //Columns
            verify(b).writeBits(0L, 64) //block timestamp
            //timestamp delta
            verify(b).writeBits(0L,27) //delta
            //first row
            verify(b).writeBits(0,64) //stored value [0]
            verify(b).writeBits(1,64) //stored value [1]
            //second row
            verify(b).writeBits(0x02, 2) //delta is within -63 to 64
            verify(b).writeBits(1 + 63, 7) //timestamp difference
            verify(b,times(2)).writeBit(true) //col1: xor is not 0 so true and new leading row so true
            verify(b).writeBits(31L, 5)
            verify(b).writeBits(33L, 6)
            verify(b).writeBits(1L, 33)
            verify(b,times(2)).writeBit(true)
            verify(b).writeBits(31L, 5)
            verify(b).writeBits(33L, 6)
            verify(b).writeBits(3L, 33)

            //close
            verify(b).writeBits(0x0F, 4)
            verify(b).writeBits(0xFFFFFFFF, 32)
            verify(b).writeBit(false) //false
        }
        //header


    }

    @Test fun compressSeries() {
        val o: ByteArrayOutputStream = ByteArrayOutputStream()
        val timestamp = 0L
        val c = CompressorFlat(timestamp, BitByteBufferWriter(o), 2)
        c.addRow(timestamp, listOf(0,1))
        c.addRow(timestamp+1, listOf(1,2))
        c.close()
        o.close()
        val i: InputStream = ByteArrayInputStream(o.toByteArray())
        val d = DecompressorFlat(BitReader(i))
        var r = d.readRows().iterator().next()
        var row = r.getRow()
        assertEquals((row.get(0).timestamp),0)
        assertEquals((row.get(0).value),0)
        assertEquals((row.get(1).value),1)
        r = d.readRows().iterator().next()
        row = r.getRow()
        assertEquals((row.get(0).timestamp),1)
        assertEquals((row.get(0).value),1)
        assertEquals((row.get(1).value),2)
    }
}

class CompressorFpcTest {
    @Test fun compressSeries() {
        val o: ByteArrayOutputStream = ByteArrayOutputStream()
        val timestamp = 1000L
        val c = CompressorFpc(timestamp, BitByteBufferWriter(o), 2)
        c.addRow(timestamp, listOf(java.lang.Double.doubleToLongBits(1.1),java.lang.Double.doubleToLongBits(2.2)))
        c.addRow(timestamp+1, listOf(java.lang.Double.doubleToLongBits(3.3),java.lang.Double.doubleToLongBits(4.4)))
        c.close()
        o.close()
        val i: InputStream = ByteArrayInputStream(o.toByteArray())
        val d = DecompressorFpc(BitReader(i))
        var r = d.readRows().iterator().next()
        var row = r.getRow()
        assertEquals((row.get(0).timestamp),1000)
        assertEquals(row.get(0).getDoubleValue(),1.1)
        assertEquals(row.get(1).getDoubleValue(),2.2)
        r = d.readRows().iterator().next()
        row = r.getRow()
        assertEquals((row.get(0).timestamp),1001)
        assertEquals(row.get(0).getDoubleValue(),3.3)
        assertEquals(row.get(1).getDoubleValue(),4.4)
    }
}