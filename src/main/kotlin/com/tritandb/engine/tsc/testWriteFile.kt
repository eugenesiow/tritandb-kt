package com.tritandb.engine.tsc

import com.tritandb.engine.experimental.valueC.CompressorFpDeltaDelta
import com.tritandb.engine.experimental.valueC.CompressorFpc
import com.tritandb.engine.experimental.valueC.DecompressorFpDeltaDelta
import com.tritandb.engine.experimental.valueC.DecompressorFpc
import com.tritandb.engine.util.BitByteBufferReader
import com.tritandb.engine.util.BitByteBufferWriter
import com.tritandb.engine.util.BitInput
import com.tritandb.engine.util.BitOutput
import kotlinx.coroutines.experimental.CommonPool
import kotlinx.coroutines.experimental.Job
import kotlinx.coroutines.experimental.launch
import kotlinx.coroutines.experimental.runBlocking
import org.mapdb.DBMaker
import org.mapdb.Serializer
import java.io.*
import java.lang.Double
import java.text.SimpleDateFormat
import kotlin.system.measureTimeMillis

/**
* TritanDb
* Created by eugene on 10/05/2017.
*/

fun main(args : Array<String>) {
    var outputFilePath = "data/shelburne.tsc"
    var filePath = "/Users/eugene/Documents/Programming/data/shelburne/shelburne.csv"
//    var filePath = "/Users/eugene/Documents/Programming/data/2016_green_taxi_trip_data_sorted.csv"
//    var outputFilePath = "data/taxi.tsc"
//    var outputFilePath = "data/"
//    var filePath = "/Users/eugene/Downloads/knoesis_observations_csv_date_sorted/"
//    var filePath = "/Users/eugene/Documents/Programming/data/shelburne/shelburne_test.csv"

//    println("Write gor flat chunk")
//    for(x in listOf(4,8,16,32,64,128,256)) {
////    for(x in 1..10) {
//        File(outputFilePath).delete()
////        File(outputFilePath).deleteRecursively()
////        File(outputFilePath).mkdir()
//        val c: CompressorFlatChunk = CompressorFlatChunk(outputFilePath, 6, 4096 * x)
////        val c: CompressorFlatChunk = CompressorFlatChunk(outputFilePath, 20)
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
////        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
////        println("${measureTimeMillis { writeFileChunkSrBench(filePath,outputFilePath,"flat", 4096 * x) }}")
////        println("$x:${folderSize(File(outputFilePath))}")
//        println("$x:${File(outputFilePath).length()}")
////    println("${measureTimeMillis { writeFileChunkSrBenchParallel(filePath,outputFilePath,"treeseq") }}")
////        File(outputFilePath).deleteRecursively()
////        File(outputFilePath).mkdir()
////        println("${measureTimeMillis { writeFileSrBench(filePath,outputFilePath,"gor") }}")
////    }
//    }

    println("Write gor flat chunk and decompress query")
//    for(x in listOf(4,8)) {
    for(x in listOf(4,8,16,32,64,128,256)) {
        File(outputFilePath).delete()
//        File(outputFilePath).deleteRecursively()
//        File(outputFilePath).mkdir()
//        val c: CompressorLSMTree = CompressorLSMTree(outputFilePath, 20, "map", 4096 * x)
//        val c: CompressorFlatChunk = CompressorFlatChunk(outputFilePath, 6, 4096 * x)
        val c: CompressorTreeSeq = CompressorTreeSeq(outputFilePath, 6, "map", 4096 * x)
        writeFileShelburne(filePath, c)
//        writeFileTaxi(filePath, c)
        for(i in 1..10) {
//            val d: Decompressor = DecompressorFlatChunk(outputFilePath)
//            val d: Decompressor = DecompressorLSMTree(outputFilePath)
            val d: Decompressor = DecompressorTree(outputFilePath)
//            val d: Decompressor = DecompressorHash(outputFilePath)
            println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//            println("${measureTimeMillis { readFileTaxi(outputFilePath, d) }}")
        }
        println()
    }

//    println("Write chunk and range query")
////    for(x in listOf(4,8)) {
//    for(x in listOf(4,8,16,32,64,128,256)) {
//        File(outputFilePath).delete()
////        File(outputFilePath).deleteRecursively()
////        File(outputFilePath).mkdir()
////        val c: CompressorLSMTree = CompressorLSMTree(outputFilePath, 20, "map", 4096 * x)
////        val c: CompressorFlatChunk = CompressorFlatChunk(outputFilePath, 20, 4096 * x)
//        val c: CompressorTreeSeq = CompressorTreeSeq(outputFilePath, 20, "map", 4096 * x)
////        writeFileShelburne(filePath, c)
//        writeFileTaxi(filePath, c)
////        queryShelburne(outputFilePath)
//        queryTaxi(outputFilePath)
//        println()
//    }

//    println("Read gor flat chunk")
////    for(x in 1..10) {
//        val d: Decompressor = DecompressorFlatChunk(outputFilePath)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
////        println("${measureTimeMillis { readFileTaxi(outputFilePath, d) }}")
////    }

//    println("Write gor tree")
////    for(x in listOf(4,8,16,32,64,128,256)) {
//    for(x in listOf(4,8,16,32)) {
////    for(x in 1..10) {
//        File(outputFilePath).delete()
////        File(outputFilePath).deleteRecursively()
////        File(outputFilePath).mkdir()
////        val c: CompressorTreeSeq = CompressorTreeSeq(outputFilePath, 6, "map", 4096 * x)
////        val c: CompressorTreeSeq = CompressorTreeSeq(outputFilePath, 20, "map", 4096 * x)
////        val c: CompressorTreeSeq = CompressorTreeSeq(outputFilePath, 6)
//        val c: CompressorTree = CompressorTree(outputFilePath, 6, "map", 4096 * x)
////        val c: CompressorTree = CompressorTree(outputFilePath, 20, "map", 4096 * x)
////        val c: CompressorLSMTreeParallel = CompressorLSMTreeParallel(outputFilePath, 6, "map", 4096 * x)
////        val c: CompressorLSMTree = CompressorLSMTree(outputFilePath, 20, "map", 4096 * x)
////        val c: CompressorLSMTreeParallel = CompressorLSMTreeParallel(outputFilePath, 20, "map", 4096 * x)
////        println("${measureTimeMillis { writeFileChunkSrBench(filePath, outputFilePath, "lsmseq") }}")
////        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
//        println("$x:${File(outputFilePath).length()}")
////        println("$x:${folderSize(File(outputFilePath))}")
////    }
//    }

//    //    println("Write LSM")
//    for(x in listOf(16)) {
////        File(outputFilePath).delete()
//        File(outputFilePath).deleteRecursively()
//        File(outputFilePath).mkdir()
//        val c: CompressorLSMTree = CompressorLSMTree(outputFilePath, 6, "map", 4096 * x)
////        val c: CompressorLSMTreeParallel = CompressorLSMTreeParallel(outputFilePath, 20, "map", 4096 * x)
////        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
//        println("$x:${folderSize(File(outputFilePath))}")
//    }

//    println("Read gor tree")
//    for(x in 1..10) {
//        val d: Decompressor = DecompressorLSMTree(outputFilePath)
////        val d: Decompressor = DecompressorTree(outputFilePath)
////        val d: Decompressor = DecompressorHash(outputFilePath)
////        println("${measureTimeMillis { readFileTaxi(outputFilePath, d) }}")
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//    }


//    println("Write gor")
////    for(x in 1..10) {
//        val o: OutputStream = File(outputFilePath).outputStream()
//        val out: BitOutput = BitByteBufferWriter(o)
//        val c: Compressor = CompressorFlat(0, out, 6)
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
//        c.close()
////    }

//    println("Read gor")
////    for(x in 1..10) {
//        var i: InputStream = File(outputFilePath).inputStream()
//        var bi: BitInput = BitByteBufferReader(i)
//        var d: Decompressor = DecompressorFlat(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
////    }

//    println("Write FPC Delta")
//    for(x in 1..10) {
//        val o = File(outputFilePath).outputStream()
//        val out = BitByteBufferWriter(o)
//        val c = CompressorFpDeltaDelta(0, out, 6)
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
//        c.close()
//    }

//    println("Read FPC Delta")
//    for(x in 1..10) {
//        var i: InputStream = File(outputFilePath).inputStream()
//        var bi: BitInput = BitByteBufferReader(i)
//        var d: Decompressor = DecompressorFpDeltaDelta(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

//
//    println("Write FPC")
//    for(x in 1..10) {
//        val o = File(outputFilePath).outputStream()
//        val out = BitByteBufferWriter(o)
//        val c = CompressorFpc(0, out, 6)
//        println("${measureTimeMillis { writeFileShelburne(filePath, c) }}")
//        c.close()
//    }

//    println("Read FPC")
//    for(x in 1..10) {
//        i = File(outputFilePath).inputStream()
//        bi = BitByteBufferReader(i)
//        d = DecompressorFpc(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

    outputFilePath = "data/taxi.tsc"
    filePath = "/Users/eugene/Documents/Programming/data/2016_green_taxi_trip_data_sorted.csv"

//    println("Write gor")
//    for(x in 1..10) {
//        val o: OutputStream = File(outputFilePath).outputStream()
//        val out: BitOutput = BitByteBufferWriter(o)
//        val c: Compressor = CompressorFlat(0, out, 20)
//        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
//        c.close()
//    }
//
//    println("Read gor")
//    for(x in 1..10) {
//        var i: InputStream = File(outputFilePath).inputStream()
//        var bi: BitInput = BitByteBufferReader(i)
//        var d: Decompressor = DecompressorFlat(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

//    println("Write FPC Delta")
//    for(x in 1..10) {
//        val o = File(outputFilePath).outputStream()
//        val out = BitByteBufferWriter(o)
//        val c = CompressorFpDeltaDelta(0, out, 20)
//        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
//        c.close()
//    }
//
//    println("Read FPC Delta")
//    for(x in 1..10) {
//        val i: InputStream = File(outputFilePath).inputStream()
//        val bi: BitInput = BitByteBufferReader(i)
//        val d: Decompressor = DecompressorFpDeltaDelta(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }
//
//    println("Write FPC")
//    for(x in 1..10) {
//        val o = File(outputFilePath).outputStream()
//        val out = BitByteBufferWriter(o)
//        val c = CompressorFpc(0, out, 20)
//        println("${measureTimeMillis { writeFileTaxi(filePath, c) }}")
//        c.close()
//    }
//
//    println("Read FPC")
//    for(x in 1..10) {
//        val i = File(outputFilePath).inputStream()
//        val bi = BitByteBufferReader(i)
//        val d = DecompressorFpc(bi)
//        println("${measureTimeMillis { readFileShelburne(outputFilePath, d) }}")
//        i.close()
//    }

    outputFilePath = "data/"
    filePath = "/Users/eugene/Downloads/knoesis_observations_csv_date_sorted/"
//    filePath = "/Users/eugene/Downloads/knoesis_empty/"

//    println("Write gor")
////    for(x in 1..10) {
//        println("${measureTimeMillis { writeFileSrBench(filePath,outputFilePath,"gor") }}")
////    }
//
//    println("Read gor")
//    for(x in 1..10) {
//        println("${measureTimeMillis { readFileSrBench(outputFilePath,"gor") }}")
//    }

//    println("Write FP Delta")
////    for(x in 1..10) {
//        println("${measureTimeMillis { writeFileSrBench(filePath,outputFilePath,"fpdelta") }}")
//    }
//
//    println("Read FP Delta")
//    for(x in 1..10) {
//        println("${measureTimeMillis { readFileSrBench(outputFilePath,"fpdelta") }}")
//    }
//
//    println("Write FPC")
//    for(x in 1..10) {
//        println("${measureTimeMillis { writeFileSrBench(filePath,outputFilePath,"fpc") }}")
//    }
//
//    println("Read FPC")
//    for(x in 1..10) {
//        println("${measureTimeMillis { readFileSrBench(outputFilePath,"fpc") }}")
//    }

}

fun folderSize(directory:File):Long {
    var length = 0L
    for (file in directory.listFiles()) {
        if (file.isFile())
            length += file.length()
        else
            length += folderSize(file)
    }
    return length
}

fun writeFileShelburne(filePath:String,c:Compressor) {
    val bw = BufferedWriter(FileWriter(filePath+".test.csv")) //write a test output
    val br = BufferedReader(FileReader(filePath))
    br.readLine() //header
    for(line in br.lines()) {
        var addThis = true
        val parts = line.split(",")
        if(parts.size>6) {
            val list = mutableListOf<Long>()
            for(i in 1..6) {
                if (parts[i] == "")
                    addThis = false
                else
                    list.add(Double.doubleToLongBits(parts[i].toDouble()))
            }

            if(addThis) {
                c.addRow((parts[0].toLong() / 1000000), list)
                //write a test output
                bw.append("${(parts[0].toLong() / 1000000)}")
                for(i in 1..6) {bw.append(",${parts[i].toDouble()}")}
                bw.append("\n")
            }

        }
    }
    br.close()
    bw.close() //write a test output
    c.close()
}

fun readFileSrBench(folderPath:String,cType:String) {
    File(folderPath).walkTopDown()
            .filter { !it.name.startsWith(".") && it.isFile && it.name.endsWith(".tsc") }.forEach {
        val i: InputStream = File(it.absolutePath).inputStream()
        val bi: BitInput = BitByteBufferReader(i)
//        println(it.absolutePath)
        var d:Decompressor = DecompressorFlat(bi)
        when(cType) {
            "gor" ->  d = DecompressorFlat(bi)
            "fpc" -> d = DecompressorFpc(bi)
            "fpdelta" -> d = DecompressorFpDeltaDelta(bi)
        }
        File("${it.absolutePath}.csv").printWriter().use { out ->
            for (r in d.readRows()) {
//                out.print("${r.timestamp}")
                for (pair in r.getRow()) {
//                    out.print(", ${pair.getDoubleValue()}")
                }
//                out.println()
            }
        }
        i.close()
    }
}

fun writeFileChunkSrBenchParallel(filePath:String, outputFilePath:String,cType:String) {
    val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val jobs = arrayListOf<Job>()
    val outSingle = outputFilePath + "data.tsc"
    val db = DBMaker
            .fileDB(outSingle)
            .fileMmapEnable()
            .make()
    File(filePath).walkTopDown()
            .filter { !it.name.startsWith(".") && it.isFile && it.name.endsWith(".csv") }.forEach {
        val br = BufferedReader(FileReader(it.absolutePath))
        val header = br.readLine().split(",") //header
        val stationName = it.name.replace(".csv","")

        jobs += launch(CommonPool) {
            val sdf = SimpleDateFormat(DATE_FORMAT)
//            val map = db.hashMap(stationName)
//                .keySerializer(Serializer.LONG)
//                .valueSerializer(Serializer.BYTE_ARRAY)
//                .createOrOpen()
            val map = db.treeMap(stationName)
                .keySerializer(Serializer.LONG)
//                .valuesOutsideNodesEnable()
                .valueSerializer(Serializer.BYTE_ARRAY)
                .createOrOpen()
            val previousVals:MutableList<Long> = mutableListOf()
            for(i in 0..header.size-1)
                previousVals.add(0)
            var c: Compressor
            when(cType) {
                else -> c = CompressorTreeSeqParallel(map, header.size-1)
            }
//        var addThis = false
            for(line in br.lines()) {
//            addThis = true
                val parts = line.split(",")
                if(parts.size>header.size-1) {
                    val list = mutableListOf<Long>()
                    val timestamp = (sdf.parse(parts[0]).time/1000)
                    for(i in 1..header.size-1) {
                        if (parts[i] == "")
                            list.add(previousVals[i])
                        else {
                            var value:Long = 0
                            if(parts[i]=="true")
                                value = 1L
                            else if(parts[i]=="false")
                                value = 0L
                            else
                                value = java.lang.Double.doubleToLongBits(parts[i].toDouble())
                            list.add(value)
                            previousVals[i] = value
                        }

                    }
                    c.addRow(timestamp,list)
                } else {
                    println(line)
                }
            }
            br.close()
            c.close()
        }
    }
    runBlocking {
        jobs.forEach {
            it.join()
            db.commit()
        }
        db.close()
    }
}

fun writeFileChunkSrBench(filePath:String, outputFilePath:String,cType:String, size:Int) {
    val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val sdf = SimpleDateFormat(DATE_FORMAT)
    File(filePath).walkTopDown()
            .filter { !it.name.startsWith(".") && it.isFile && it.name.endsWith(".csv") }.forEach {
        val br = BufferedReader(FileReader(it.absolutePath))
        val header = br.readLine().split(",") //header
        val stationName = it.name.replace(".csv","")
        val outSingle = outputFilePath + "data.tsc"
        val out = outputFilePath + stationName + ".tsc"
        val previousVals:MutableList<Long> = mutableListOf()
        for(i in 0..header.size-1)
            previousVals.add(0)
        var c: Compressor
        when(cType) {
//            "flat" ->  c = CompressorFlatChunk(out, header.size-1)
            "tree" -> c = CompressorTree(outSingle, header.size-1, stationName, size)
            "treeseq" -> c = CompressorTreeSeq(outSingle, header.size-1, stationName, size)
            else -> c = CompressorFlatChunk(out, header.size-1,size)
        }
//        var addThis = false
        for(line in br.lines()) {
//            addThis = true
            val parts = line.split(",")
            if(parts.size>header.size-1) {
                val list = mutableListOf<Long>()
                val timestamp = (sdf.parse(parts[0]).time/1000)
                for(i in 1..header.size-1) {
                    if (parts[i] == "")
                        list.add(previousVals[i])
                    else {
                        var value:Long = 0
                        if(parts[i]=="true")
                            value = 1L
                        else if(parts[i]=="false")
                            value = 0L
                        else
                            value = java.lang.Double.doubleToLongBits(parts[i].toDouble())
                        list.add(value)
                        previousVals[i] = value
                    }

                }
                c.addRow(timestamp,list)
            } else {
                println(line)
            }
        }
        br.close()
        c.close()
    }
}

fun writeFileSrBench(filePath:String, outputFilePath:String,cType:String) {
    val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val sdf = SimpleDateFormat(DATE_FORMAT)
    File(filePath).walkTopDown()
            .filter { !it.name.startsWith(".") && it.isFile && it.name.endsWith(".csv") }.forEach {
        val br = BufferedReader(FileReader(it.absolutePath))
        val header = br.readLine().split(",") //header
        val stationName = it.name.replace(".csv","")
        val o: OutputStream = File(outputFilePath + stationName + ".tsc").outputStream()
        val out: BitOutput = BitByteBufferWriter(o)
        val previousVals:MutableList<Long> = mutableListOf()
        for(i in 0..header.size-1)
            previousVals.add(0)
        var c: Compressor = CompressorFlat(0, out, header.size-1)
        when(cType) {
            "gor" ->  c = CompressorFlat(0, out, header.size-1)
            "fpc" -> c = CompressorFpc(0, out, header.size-1)
            "fpdelta" -> c = CompressorFpDeltaDelta(0, out, header.size-1)
        }
        var addThis = false
        for(line in br.lines()) {
            addThis = true
            val parts = line.split(",")
            if(parts.size>header.size-1) {
                val list = mutableListOf<Long>()
                val timestamp = (sdf.parse(parts[0]).time/1000)
                for(i in 1..header.size-1) {
                    if (parts[i] == "")
                        list.add(previousVals[i])
                    else {
                        var value:Long = 0
                        if(parts[i]=="true")
                            value = 1L
                        else if(parts[i]=="false")
                            value = 0L
                        else
                            value = java.lang.Double.doubleToLongBits(parts[i].toDouble())
                        list.add(value)
                        previousVals[i] = value
                    }

                }
                c.addRow(timestamp,list)
            } else {
                println(line)
            }
        }
        br.close()
        c.close()
    }
}

fun writeFileTaxi(filePath:String, c:Compressor) {
    val DATE_FORMAT = "yyyy-MM-dd HH:mm:ss"
    val sdf = SimpleDateFormat(DATE_FORMAT)
    val br = BufferedReader(FileReader(filePath))
    br.readLine() //header
    for(line in br.lines()) {
        val parts = line.split(",")
        if(parts.size>21) {
            val timestamp = (sdf.parse(parts[1]).time/1000)
            val list = mutableListOf<Long>()
            list.add(parts[0].toLong())
            list.add(sdf.parse(parts[2]).time/1000)
            if(parts[3]=="true")
                list.add(1)
            else
                list.add(0)
            list.add(parts[4].toLong())
            list.add(Double.doubleToLongBits(parts[5].toDouble()))
            list.add(Double.doubleToLongBits(parts[6].toDouble()))
            list.add(Double.doubleToLongBits(parts[7].toDouble()))
            list.add(Double.doubleToLongBits(parts[8].toDouble()))
            list.add(parts[9].toLong())
            list.add(Double.doubleToLongBits(parts[10].toDouble()))
            list.add(Double.doubleToLongBits(parts[11].toDouble()))
            list.add(Double.doubleToLongBits(parts[12].toDouble()))
            list.add(Double.doubleToLongBits(parts[13].toDouble()))
            list.add(Double.doubleToLongBits(parts[14].toDouble()))
            list.add(Double.doubleToLongBits(parts[15].toDouble()))
            list.add(Double.doubleToLongBits(parts[17].toDouble()))
            list.add(Double.doubleToLongBits(parts[18].toDouble()))
            list.add(parts[19].toLong())
            if(parts[20].isEmpty())
                list.add(1)
            else
                list.add(parts[20].toLong())
            list.add(sdf.parse(parts[21]).time/1000)
            c.addRow(timestamp,list)
        }
    }
    br.close()
    c.close()
}

fun readFileTaxi(filePath:String, d:Decompressor) { //works for each
//    var count = 0
    File("${filePath}.csv").printWriter().use { out ->
        for (r in d.readRows()) {
//            count++
//            println("${r.timestamp}")
//            out.print("${r.timestamp}")
            for (pair in r.getRow()) {
//                out.print(", ${pair.getDoubleValue()}")
            }
//            out.println()
        }
//        println(count)
    }
}

fun readFileShelburne(filePath:String, d:Decompressor) { //works for each
//    var count = 0
    File("${filePath}.csv").printWriter().use { out ->
        for (r in d.readRows()) {
//            println("${r.timestamp}")
//            out.print("${r.timestamp}")
            for (pair in r.getRow()) {
//                out.print(",${pair.getDoubleValue()}")
            }
//            out.println()
        }
    }
}