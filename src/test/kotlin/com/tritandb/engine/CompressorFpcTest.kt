package com.tritandb.engine

import com.tritandb.engine.experimental.valueC.CompressorFpc
import com.tritandb.engine.experimental.valueC.DecompressorFpc
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitReader
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import org.jetbrains.spek.api.dsl.on
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.io.InputStream
import kotlin.test.assertEquals

/**
 * Created by eugenesiow on 07/06/2017.
 */

class CompressorFpcTest: Spek( {
    describe("Test to compress a series of FP values") {
        on("encoding a stream of rows of FP values") {
            val o: ByteArrayOutputStream = ByteArrayOutputStream()
            val timestamp = 1000L
            val c = CompressorFpc(timestamp, BitByteBufferWriter(o), 2)
            c.addRow(timestamp, listOf(java.lang.Double.doubleToLongBits(1.1), java.lang.Double.doubleToLongBits(2.2)))
            c.addRow(timestamp + 1, listOf(java.lang.Double.doubleToLongBits(3.3), java.lang.Double.doubleToLongBits(4.4)))
            c.close()
            o.close()
            it("should decode a similar sequence of rows of FP values") {
                val i: InputStream = ByteArrayInputStream(o.toByteArray())
                val d = DecompressorFpc(BitReader(i))
                var r = d.readRows().iterator().next()
                var row = r.getRow()
                assertEquals((row.get(0).timestamp), 1000)
                assertEquals(row.get(0).getDoubleValue(), 1.1)
                assertEquals(row.get(1).getDoubleValue(), 2.2)
                r = d.readRows().iterator().next()
                row = r.getRow()
                assertEquals((row.get(0).timestamp), 1001)
                assertEquals(row.get(0).getDoubleValue(), 3.3)
                assertEquals(row.get(1).getDoubleValue(), 4.4)
            }
        }
    }
})