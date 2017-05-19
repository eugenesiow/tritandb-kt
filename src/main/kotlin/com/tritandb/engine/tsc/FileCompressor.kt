package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.CompressorFlat
import com.tritandb.engine.util.BitOutput
import java.io.OutputStream

/**
 * TritanDb
 * Created by eugene on 18/05/2017.
 */
data class FileCompressor(val compressor: Compressor, val bitoutput: BitOutput, val outputstream: OutputStream) {
}