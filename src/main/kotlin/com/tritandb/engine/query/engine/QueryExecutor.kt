package com.tritandb.engine.query.engine

import com.google.gson.Gson
import com.natpryce.konfig.Configuration
import com.natpryce.konfig.PropertyGroup
import com.natpryce.konfig.getValue
import com.natpryce.konfig.stringType
import com.tritandb.engine.query.op.RangeFlatChunk
import com.tritandb.engine.tsc.data.Row
import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.OpWalker
import java.io.File
import kotlin.coroutines.experimental.buildIterator
import kotlin.system.measureTimeMillis


/**
 * Created by eugenesiow on 18/06/2017.
 */
class QueryExecutor(private val config: Configuration) {
    private val model = loadData()
    private object server : PropertyGroup() {
        val dataDir by stringType
        val modelDir by stringType
    }
    private val meta = loadMetaData()

    fun query(queryString:String):Iterator<Row> {
        val query = QueryFactory.create(queryString)
        val op = Algebra.compile(query)
//        println(op)

        val v = SparqlOpVisitor()
        v.setModel(model)
        println("${measureTimeMillis{OpWalker.walk(op, v)}}")

        var finalItr:Iterator<Row> = buildIterator {}
        v.getPlan().forEach { (a,b)->
            if(b is RangeFlatChunk) {
//                b as RangeFlatChunk
                val metaData = meta[a]
                var showTimestamp = false
                val colsProject = mutableListOf<Int>()
                for (colName in b.cols) {
                    if (colName == metaData!!.timestamp)
                        showTimestamp = true
                    val colIdx = metaData.columns.indexOf(colName)
                    if (colIdx >= 0)
                        colsProject.add(colIdx)
                }
//            println(metaData)
//            println(b.cols)
                var itr = b.execute()
                if (b.aggregates.isNotEmpty()) {
                    itr = buildIterator {
                        for ((colName, aggrFun) in b.aggregates) {
                            val colIdx = metaData!!.columns.indexOf(colName.replace("$a.", ""))
                            if (colIdx >= 0) {
                                if (aggrFun[0].first.toUpperCase() == "AVG") {
                                    colsProject.add(0)
                                    yieldAll(b.avgRun(b.start, b.end, colIdx))
                                }
                            }
                        }
                    }
                }
                finalItr = buildIterator {
                    itr.forEach { (timestamp, values) ->
                        val newValues = values
                                .filterIndexed { index, _ -> colsProject.contains(index) }
                                .toMutableList()
                        yield(Row(timestamp, newValues.toLongArray()))
//                row -> yield(row)
                    }
                }
            } else {
                finalItr = b.execute()
            }
        }

        return finalItr

    }

    private fun loadMetaData():Map<String,Meta> {
        val metadata = mutableMapOf<String,Meta>()
        val gson = Gson()
        File(config[server.dataDir]).walk().forEach {
            if(it.name.endsWith(".json")) {
                val name = it.name.replace(".json","")
                val obj = gson.fromJson(it.readText(),Meta::class.java)
                metadata.put(name,obj)
            }
        }
        return metadata
    }

    private fun loadData(): Model {
        val model = ModelFactory.createDefaultModel()
        RDFDataMgr.read(model, File("${config[server.modelDir]}/shelburne.ttl").inputStream(), RDFLanguages.TURTLE)
        return model
    }
}