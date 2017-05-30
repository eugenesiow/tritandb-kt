package com.tritandb.engine

import com.nhaarman.mockito_kotlin.*
import com.tritandb.engine.experimental.*
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

class CompressorTsTest {
    @Test fun compressTsDelta_RLE_LEB128() {
        val o: ByteArrayOutputStream = ByteArrayOutputStream()
        val timestamp = 1496150168244L
        val c = CompressorDelta(timestamp, BitByteBufferWriter(o), 0)
        c.addRow(timestamp, listOf())
        c.addRow(timestamp+1000, listOf())
        c.addRow(timestamp+2000, listOf())
        c.addRow(timestamp+3000, listOf())
        c.addRow(timestamp+4501, listOf())
        c.addRow(timestamp+6000, listOf())
        c.close()
        o.close()
        val i: InputStream = ByteArrayInputStream(o.toByteArray())
        val d = DecompressorDelta(BitReader(i))
        val it = d.readRows().iterator()
        var r = it.next()
        assertEquals(r.timestamp,timestamp)
        r = it.next()
        assertEquals(r.timestamp,timestamp+1000)
        r = it.next()
        assertEquals(r.timestamp,timestamp+2000)
        r = it.next()
        assertEquals(r.timestamp,timestamp+3000)
        r = it.next()
        assertEquals(r.timestamp,timestamp+4501)
        r = it.next()
        assertEquals(r.timestamp,timestamp+6000)
    }

    @Test fun compressTsDelta_RLE_Rice() {
        val o: ByteArrayOutputStream = ByteArrayOutputStream()
        val timestamp = 1496150168244L
        val c = CompressorDeltaRice(timestamp, BitByteBufferWriter(o), 0)
        c.addRow(timestamp, listOf())
        c.addRow(timestamp+1000, listOf())
        c.addRow(timestamp+2000, listOf())
        c.addRow(timestamp+3000, listOf())
        c.addRow(timestamp+4501, listOf())
        c.addRow(timestamp+6000, listOf())
        c.addRow(timestamp+6005, listOf())
        c.addRow(timestamp+6010, listOf())
        c.addRow(timestamp+100000-9990, listOf())
        c.addRow(timestamp+100000, listOf())
        for(i in 1..7)
            c.addRow(timestamp+100000+(i*10000), listOf())
        c.close()
        o.close()
        val i: InputStream = ByteArrayInputStream(o.toByteArray())
        val d = DecompressorDeltaRice(BitReader(i))
        val it = d.readRows().iterator()
        var r = it.next()
        assertEquals(r.timestamp,timestamp)
        r = it.next()
        assertEquals(r.timestamp,timestamp+1000)
        r = it.next()
        assertEquals(r.timestamp,timestamp+2000)
        r = it.next()
        assertEquals(r.timestamp,timestamp+3000)
        r = it.next()
        assertEquals(r.timestamp,timestamp+4501)
        r = it.next()
        assertEquals(r.timestamp,timestamp+6000)
        r = it.next()
        assertEquals(r.timestamp,timestamp+6005)
        r = it.next()
        assertEquals(r.timestamp,timestamp+6010)
        r = it.next()
        assertEquals(r.timestamp,timestamp+100000-9990)
        r = it.next()
        assertEquals(r.timestamp,timestamp+100000)
        for(i in 1..7) {
            r = it.next()
            assertEquals(r.timestamp,timestamp+100000+(i*10000))
        }
    }

    @Test fun compressTsDelta_RLE_Rice_2() {
        val o: ByteArrayOutputStream = ByteArrayOutputStream()
        val timestamp = 1271692742104L
        val c = CompressorDeltaRice(timestamp, BitByteBufferWriter(o), 0)
        c.addRow(timestamp, listOf())
        c.addRow(1271692752104, listOf())
        c.addRow(1271692762114, listOf())
        c.addRow(1271692772114, listOf())
        c.addRow(1271692782104, listOf())
        c.addRow(1271692792114, listOf())
        c.addRow(1271692802114, listOf())
        c.addRow(1271692812094, listOf())
        c.addRow(1271692822094, listOf())
        c.addRow(1271692832094, listOf())
        c.addRow(1271692842094, listOf())
        c.addRow(1271692852104, listOf())
        c.addRow(1271692862094, listOf())
        for(i in 1..7)
            c.addRow(1271692862094+(i*10000), listOf())
        c.addRow(1271692942104, listOf())
        c.close()
        o.close()
        val i: InputStream = ByteArrayInputStream(o.toByteArray())
        val d = DecompressorDeltaRice(BitReader(i))
        val it = d.readRows().iterator()
        var r = it.next()
        assertEquals(r.timestamp,timestamp)
        r = it.next()
        assertEquals(r.timestamp,1271692752104)
        r = it.next()
        assertEquals(r.timestamp,1271692762114)
        r = it.next()
        assertEquals(r.timestamp,1271692772114)

        r = it.next()
        assertEquals(r.timestamp,1271692782104)
        r = it.next()
        assertEquals(r.timestamp,1271692792114)
        r = it.next()
        assertEquals(r.timestamp,1271692802114)
        r = it.next()
        assertEquals(r.timestamp,1271692812094)
        r = it.next()
        assertEquals(r.timestamp,1271692822094)
        r = it.next()
        assertEquals(r.timestamp,1271692832094)
        r = it.next()
        assertEquals(r.timestamp,1271692842094)
        r = it.next()
        assertEquals(r.timestamp,1271692852104)
        r = it.next()
        assertEquals(r.timestamp,1271692862094)

        for(i in 1..7) {
            r = it.next()
            assertEquals(r.timestamp,1271692862094+(i*10000))
        }
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