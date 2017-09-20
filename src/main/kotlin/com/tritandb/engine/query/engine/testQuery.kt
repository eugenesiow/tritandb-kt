package com.tritandb.engine.query.engine

import java.text.SimpleDateFormat
import java.util.*


/**
 * Created by eugenesiow on 18/06/2017.
 */

val colNamesShelburne = arrayOf("AirTemperature", "SolarRadiation", "SoilMoisture", "LeafWetness", "InternalTemperature", "RelativeHumidity")

//fun main(args : Array<String>) {
////    shelburneCol(args[0].toInt(),args[1].toInt())
//    shelburneCol(1,1)
//}
//
//fun shelburneCol(num:Int,queryType:Int) {
//
//    var fixedSeed = 30L
//    val rand = Random(fixedSeed)
//    fixedSeed = 20L
//    val randCol = Random(fixedSeed)
//    val max = 1406141325958
//    val min = 1271692742104
//    val range = ((max + 1 - min )/100).toInt()
//    var start = 0L
//    var end = 0L
//    var col = 0
//    for(i in 1..num) {
//        val a = (rand.nextInt(range)) * 100L + min
//        val b = (rand.nextInt(range)) * 100L + min
//        start = a
//        end = b
//        if (a > b) {
//            start = b
//            end = a
//        }
//        col = randCol.nextInt(colNamesShelburne.size)
////        shelburneColQuery(start,end,col)
//
//    }
//    when(queryType) {
//        1->shelburneColQuery(start,end,col)
//        2->shelburneAggrQuery(start,end,col)
//        3->shelburneRowQuery(start,end)
//    }
//}
//
//fun shelburneColQuery(start:Long,end:Long,col:Int) {
//    val QUDT = "http://qudt.org/2.0/schema/qudt/"
//    val SOSA = "http://www.w3.org/ns/sosa/"
//    val TIME = "http://www.w3.org/2006/time#"
//
//    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
//    val s = sdf.format(Date(start))
//    val e = sdf.format(Date(end))
//
//    val q = QueryExecutor()
//    val queryString = "PREFIX qudt: <$QUDT>\n" +
//            "PREFIX sosa: <$SOSA>\n" +
//            "PREFIX time: <$TIME>\n" +
//            "SELECT ?v ?timeVal\n"+
//            "{  ?obs sosa:hasResult ?result;\n" +
//            "       sosa:observedProperty <http://tritandb.com/ns/iot/${colNamesShelburne[col]}>;\n" +
//            "       sosa:resultTime ?time.\n" +
//            "   ?result qudt:numericValue ?v.\n" +
//            "   ?time time:inXSDDateTimeStamp ?timeVal.\n"+
//            "   FILTER(?timeVal>\"$s\" && ?timeVal<\"$e\")\n"+
//            "}"
////    println(queryString)
//    q.query(queryString)
//}
//
//fun shelburneRowQuery(start:Long,end:Long) {
//    val QUDT = "http://qudt.org/2.0/schema/qudt/"
//    val SOSA = "http://www.w3.org/ns/sosa/"
//    val TIME = "http://www.w3.org/2006/time#"
//
//    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
//    val s = sdf.format(Date(start))
//    val e = sdf.format(Date(end))
//
//    val sel = mutableListOf<String>()
//    val join = mutableListOf<String>()
//    for(colName in colNamesShelburne) {
//        join += "{  ?obs$colName sosa:hasResult ?result$colName;\n" +
//                "       sosa:observedProperty <http://tritandb.com/ns/iot/$colName>;\n" +
//                "       sosa:resultTime ?time.\n" +
//                "   ?result$colName qudt:numericValue ?v$colName.}\n"
//        sel += "?v$colName"
//    }
//    val joins = join.joinToString(" UNION ")
//    val sels = sel.joinToString(" ")
//
//    val q = QueryExecutor()
//    val queryString = "PREFIX qudt: <$QUDT>\n" +
//            "PREFIX sosa: <$SOSA>\n" +
//            "PREFIX time: <$TIME>\n" +
//            "SELECT ?timeVal $sels WHERE\n"+
//            "{ $joins" +
//            "   ?time time:inXSDDateTimeStamp ?timeVal.\n"+
//            "   FILTER(?timeVal>\"$s\" && ?timeVal<\"$e\")\n"+
//            "}"
////    println(queryString)
//    q.query(queryString)
//}
//
//fun shelburneAggrQuery(start:Long,end:Long,col:Int) {
//    val QUDT = "http://qudt.org/2.0/schema/qudt/"
//    val SOSA = "http://www.w3.org/ns/sosa/"
//    val TIME = "http://www.w3.org/2006/time#"
//
//    val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
//    val s = sdf.format(Date(start))
//    val e = sdf.format(Date(end))
//
//    val q = QueryExecutor()
//    val queryString = "PREFIX qudt: <$QUDT>\n" +
//            "PREFIX sosa: <$SOSA>\n" +
//            "PREFIX time: <$TIME>\n" +
//            "SELECT (AVG(?v) as ?avgV)\n"+
//            "{  ?obs sosa:hasResult ?result;\n" +
//            "       sosa:observedProperty <http://tritandb.com/ns/iot/${colNamesShelburne[col]}>;\n" +
//            "       sosa:resultTime ?time.\n" +
//            "   ?result qudt:numericValue ?v.\n" +
//            "   ?time time:inXSDDateTimeStamp ?timeVal.\n"+
//            "   FILTER(?timeVal>\"$s\" && ?timeVal<\"$e\")\n"+
//            "}"
////    println(queryString)
//    q.query(queryString)
//}