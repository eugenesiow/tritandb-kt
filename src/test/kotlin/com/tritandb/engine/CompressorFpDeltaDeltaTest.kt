package com.tritandb.engine

import com.tritandb.engine.experimental.valueC.CompressorFpDeltaDelta
import com.tritandb.engine.experimental.valueC.DecompressorFpDeltaDelta
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
 * TritanDb
 * Created by eugene on 08/06/2017.
 */
class CompressorFpDeltaDeltaTest: Spek( {
    describe("Test to compress a series of FP values with delta detla") {
        on("encoding a stream of rows of FP values") {
            val o: ByteArrayOutputStream = ByteArrayOutputStream()
            val timestamp = 1000L
            val c = CompressorFpDeltaDelta(timestamp, BitByteBufferWriter(o), 2)
            c.addRow(timestamp, listOf(java.lang.Double.doubleToLongBits(1.1), java.lang.Double.doubleToLongBits(2.2)))
            c.addRow(timestamp + 1, listOf(java.lang.Double.doubleToLongBits(3.3), java.lang.Double.doubleToLongBits(4.4)))
            c.close()
            o.close()
            it("should decode a similar sequence of rows of FP values") {
                val i: InputStream = ByteArrayInputStream(o.toByteArray())
                val d = DecompressorFpDeltaDelta(BitReader(i))
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

    describe("Test to compress a complrex series of FP values with delta delta") {
        on("encoding a stream of rows of FP values") {
            val o: ByteArrayOutputStream = ByteArrayOutputStream()
            val c = CompressorFpDeltaDelta(1271692742104340000, BitByteBufferWriter(o), 6)
            c.addRow(1271692742104340000, listOf(java.lang.Double.doubleToLongBits(47.42279624938965), java.lang.Double.doubleToLongBits(847.8090209960938), java.lang.Double.doubleToLongBits(2439.60009765625), java.lang.Double.doubleToLongBits(0.0), java.lang.Double.doubleToLongBits(60.35), java.lang.Double.doubleToLongBits(46.18999862670898)))
            c.addRow(1271692752104312000, listOf(java.lang.Double.doubleToLongBits(48.4492094039917), java.lang.Double.doubleToLongBits(816.0280151367188), java.lang.Double.doubleToLongBits(2439.6201171875), java.lang.Double.doubleToLongBits(0.0), java.lang.Double.doubleToLongBits(60.35), java.lang.Double.doubleToLongBits(46.36000061035156)))
            c.addRow(1271692762114620000, listOf(java.lang.Double.doubleToLongBits(48.56946849822998), java.lang.Double.doubleToLongBits(882.18701171875), java.lang.Double.doubleToLongBits(2438.889892578125), java.lang.Double.doubleToLongBits(0.0), java.lang.Double.doubleToLongBits(60.35), java.lang.Double.doubleToLongBits(46.45000076293945)))
            c.close()
            o.close()
            it("should decode a similar sequence of rows of FP values") {
                val i: InputStream = ByteArrayInputStream(o.toByteArray())
                val d = DecompressorFpDeltaDelta(BitReader(i))
                var r = d.readRows().iterator().next()
                var row = r.getRow()
                assertEquals((row.get(0).timestamp), 1271692742104340000)
                assertEquals(row.get(0).getDoubleValue(), 47.42279624938965)
                assertEquals(row.get(1).getDoubleValue(), 847.8090209960938)
                r = d.readRows().iterator().next()
                row = r.getRow()
                assertEquals((row.get(0).timestamp), 1271692752104312000)
                assertEquals(row.get(0).getDoubleValue(), 48.4492094039917)
                assertEquals(row.get(1).getDoubleValue(), 816.0280151367188)
                assertEquals(row.get(2).getDoubleValue(), 2439.6201171875)
                assertEquals(row.get(3).getDoubleValue(), 0.0)
                assertEquals(row.get(4).getDoubleValue(), 60.35)
                assertEquals(row.get(5).getDoubleValue(), 46.36000061035156)
                r = d.readRows().iterator().next()
                row = r.getRow()
                assertEquals((row.get(0).timestamp), 1271692762114620000)
                assertEquals(row.get(0).getDoubleValue(), 48.56946849822998)
                assertEquals(row.get(1).getDoubleValue(), 882.18701171875)
                assertEquals(row.get(2).getDoubleValue(), 2438.889892578125)
                assertEquals(row.get(3).getDoubleValue(), 0.0)
                assertEquals(row.get(4).getDoubleValue(), 60.35)
                assertEquals(row.get(5).getDoubleValue(), 46.45000076293945)
            }
        }
    }
})