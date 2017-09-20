package com.tritandb.engine.query.engine

import com.google.gson.Gson
import com.natpryce.konfig.*
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

        v.getPlan().forEach { (a,b)->
            b as RangeFlatChunk
            println(b.cols)
            b.execute()
            return buildIterator { b.iterator.forEach { it->
                yield(it)
            }}
        }

        return buildIterator {}

    }

    private fun loadMetaData():Map<String,String> {
        val metadata = mutableMapOf<String,String>()
        val gson = Gson()
        File(config[server.dataDir]).walk().forEach {
            if(it.name.endsWith(".json")) {
                val name = it.name.replace(".json","")
//                gson.fromJson(it.readText(),Meta.class)
                metadata.put(name,"")
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