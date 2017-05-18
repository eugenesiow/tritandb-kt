package com.tritandb.engine

import com.nhaarman.mockito_kotlin.*
import com.tritandb.engine.tsc.CompressorFlat
import com.tritandb.engine.util.BitByteBufferWriter
import org.junit.Test

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
}
