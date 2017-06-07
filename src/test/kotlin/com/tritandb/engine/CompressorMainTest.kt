package com.tritandb.engine

import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe

import com.nhaarman.mockito_kotlin.*
import com.tritandb.engine.tsc.CompressorFlat
import com.tritandb.engine.tsc.DecompressorFlat
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitReader
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import kotlin.test.assertEquals

/**
 * Created by eugenesiow on 07/06/2017.
 */
class CompressorMainTest: Spek( {
    describe("Test mocking the bit writing buffer of the compressor") {
        on("adding these rows of timestamps and values") {
            val b: BitByteBufferWriter = mock()
//        whenever(b.writeBits(any(),any())).then { println(it) }
//        whenever(b.writeBit(any())).then { println(it) }
            val timestamp = 0L
            val c = CompressorFlat(timestamp, b, 2)
            c.addRow(timestamp, listOf(0, 1))
            c.addRow(timestamp + 1, listOf(1, 2))
            c.close()

            it("should write call writebits and writebit so many times and in this order") {
                verify(b, times(15)).writeBits(any(), any())
                verify(b, times(5)).writeBit(any())

                inOrder(b) {
                    verify(b).writeBits(2L, 32) //Columns
                    verify(b).writeBits(0L, 64) //block timestamp
                    //timestamp delta
                    verify(b).writeBits(0L, 27) //delta
                    //first row
                    verify(b).writeBits(0, 64) //stored value [0]
                    verify(b).writeBits(1, 64) //stored value [1]
                    //second row
                    verify(b).writeBits(0x02, 2) //delta is within -63 to 64
                    verify(b).writeBits(1 + 63, 7) //timestamp difference
                    verify(b, times(2)).writeBit(true) //col1: xor is not 0 so true and new leading row so true
                    verify(b).writeBits(31L, 5)
                    verify(b).writeBits(33L, 6)
                    verify(b).writeBits(1L, 33)
                    verify(b, times(2)).writeBit(true)
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


    }

    describe("Test compressing a series of values and timestamps") {
        on("encoding these rows of timestamps and values") {
            val o: ByteArrayOutputStream = ByteArrayOutputStream()
            val timestamp = 0L
            val c = CompressorFlat(timestamp, BitByteBufferWriter(o), 2)
            c.addRow(timestamp, listOf(0, 1))
            c.addRow(timestamp + 1, listOf(1, 2))
            c.close()
            o.close()
            it("should decode the same sequence of rows of timestamps and values") {
                val i: InputStream = ByteArrayInputStream(o.toByteArray())
                val d = DecompressorFlat(BitReader(i))
                var r = d.readRows().iterator().next()
                var row = r.getRow()
                assertEquals((row.get(0).timestamp), 0)
                assertEquals((row.get(0).value), 0)
                assertEquals((row.get(1).value), 1)
                r = d.readRows().iterator().next()
                row = r.getRow()
                assertEquals((row.get(0).timestamp), 1)
                assertEquals((row.get(0).value), 1)
                assertEquals((row.get(1).value), 2)
            }
        }
    }
})