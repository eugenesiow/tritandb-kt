package com.tritandb.engine.query.engine

/**
 * Created by eugenesiow on 18/06/2017.
 */

fun main(args : Array<String>) {
    val QUDT = "http://qudt.org/2.0/schema/qudt/"
    val SOSA = "http://www.w3.org/ns/sosa/"
    val TIME = "http://www.w3.org/2006/time#"

    val q = QueryExecutor()
    val queryString = "PREFIX qudt: <$QUDT>\n" +
            "PREFIX sosa: <$SOSA>\n" +
            "PREFIX time: <$TIME>\n" +
            "SELECT ?v ?prop ?timeVal\n"+
            "{  ?obs sosa:hasResult ?result;\n" +
            "       sosa:observedProperty ?prop;\n" +
            "       sosa:resultTime ?time.\n" +
            "   ?result qudt:numericValue ?v.\n" +
            "   ?time time:inXSDDateTimeStamp ?timeVal.\n"+
            "   FILTER(?timeVal>\"2003-04-01T00:00:00\" && ?timeVal<\"2003-04-01T01:00:00\")\n"+
            "}"
    q.query(queryString)

}