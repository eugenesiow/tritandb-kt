package com.tritandb.engine

import com.tritandb.engine.experimental.timestampC.CompressorDelta
import org.jetbrains.spek.api.Spek
import org.jetbrains.spek.api.dsl.describe
import org.jetbrains.spek.api.dsl.it
import java.io.ByteArrayOutputStream
import com.tritandb.engine.experimental.timestampC.CompressorDeltaRice
import com.tritandb.engine.experimental.timestampC.DecompressorDelta
import com.tritandb.engine.experimental.timestampC.DecompressorDeltaRice
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitReader
import org.jetbrains.spek.api.dsl.on
import java.io.ByteArrayInputStream
import java.io.InputStream
import kotlin.test.assertEquals

/**
 * Created by eugenesiow on 07/06/2017.
 */
class CompressorTsTest: Spek({
   describe("Test Delta_RLE_LEB128 timestamp encoding") {
        on("encoding a sequence of timestamps with varying deltas") {
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
            it("should decode the same sequence of timestamps") {
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

        }

    }

    describe("Test Delta_RLE_RICE timestamp encoding") {

        on("encoding a sequence of timestamps with varying deltas") {
            val o: ByteArrayOutputStream = ByteArrayOutputStream()
            val timestamp = 1496150168244L
            val c = CompressorDeltaRice(timestamp, BitByteBufferWriter(o), 0)
            c.addRow(timestamp, listOf())
            c.addRow(timestamp + 1000, listOf())
            c.addRow(timestamp + 2000, listOf())
            c.addRow(timestamp + 3000, listOf())
            c.addRow(timestamp + 4501, listOf())
            c.addRow(timestamp + 6000, listOf())
            c.addRow(timestamp + 6005, listOf())
            c.addRow(timestamp + 6010, listOf())
            c.addRow(timestamp + 100000 - 9990, listOf())
            c.addRow(timestamp + 100000, listOf())
            for(i in 1..7)
                c.addRow(timestamp+100000+(i*10000), listOf())
            c.close()
            o.close()
            val i: InputStream = ByteArrayInputStream(o.toByteArray())
            val d = DecompressorDeltaRice(BitReader(i))
            val it = d.readRows().iterator()
            it("should decode the same sequence of timestamps") {
                var r = it.next()
                assertEquals(r.timestamp, timestamp)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 1000)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 2000)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 3000)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 4501)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 6000)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 6005)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 6010)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 100000 - 9990)
                r = it.next()
                assertEquals(r.timestamp, timestamp + 100000)
                for (i in 1..7) {
                    r = it.next()
                    assertEquals(r.timestamp, timestamp + 100000 + (i * 10000))
                }
            }
        }


    }

    describe("Test Delta_RLE_RICE timestamp encoding with an actual IoT stream of timestamps from Shelburne") {

        on("encoding a sequence of timestamps with varying deltas") {
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
            it("should return the same sequence of timestamps") {
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



    }
})