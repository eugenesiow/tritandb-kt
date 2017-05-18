package com.tritandb.engine

import com.nhaarman.mockito_kotlin.inOrder
import com.nhaarman.mockito_kotlin.mock
import com.tritandb.engine.tsc.CompressorFlat
import com.tritandb.engine.util.BitByteBufferWriter
import org.junit.Test
import org.mockito.Mockito


/**
 * TritanDb
 * Created by eugene on 18/05/2017.
 */

class CompressorTest {
    @Test fun compressRow_writeBytes() {
        val b: BitByteBufferWriter = mock()
        val timestamp = 0L
        val c = CompressorFlat(timestamp, b, 2)
        c.addRow(timestamp, listOf(0,1))
        c.addRow(timestamp+1, listOf(1,2))
        c.close()

        inOrder(b) {
            verify(b).writeBits(2L, 32) //Columns
            verify(b).writeBits(0L, 64) //block timestamp
            //timestamp delta
            verify(b).writeBits(0L,27) //delta
            //first row
            verify(b).writeBits(0,64) //stored value [0]
            verify(b).writeBits(1,64) //stored value [1]

            //TODO: second row

            //close
            verify(b).writeBits(0x0F, 4)
            verify(b).writeBits(0xFFFFFFFF, 32)
            verify(b).writeBit(false) //false
        }
        //header


    }
}
