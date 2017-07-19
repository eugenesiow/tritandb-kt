package com.tritandb.engine.query.engine

import org.apache.jena.query.QueryFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.rdf.model.ModelFactory
import org.apache.jena.riot.RDFDataMgr
import org.apache.jena.riot.RDFLanguages
import org.apache.jena.sparql.algebra.Algebra
import org.apache.jena.sparql.algebra.OpWalker
import java.io.File
import kotlin.system.measureTimeMillis


/**
 * Created by eugenesiow on 18/06/2017.
 */
class QueryExecutor {

    fun query(queryString:String) {
        val query = QueryFactory.create(queryString)
        val op = Algebra.compile(query)
        println(op)

        val v = SparqlOpVisitor()
        v.setModel(loadData())
        println("Walk Time: ${measureTimeMillis{OpWalker.walk(op, v)}}")

        // The stage generator to be used for a query execution
        // is read from the context.  There is a global context, which
        // is cloned when a query execution object (query engine) is
        // created.

        // Normally, StageGenerators are chained - a new one inspects the
        // execution request and sees if it handles it.  If it does not,
        // it sends the request to the stage generator that was already registered.

        // The normal stage generator is registerd in the global context.
        // This can be replaced, so that every query execution uses the
        // alternative stage generator, or the cloned context can be
        // alter so that just one query execution is affected.

        // Change the stage generator for all queries ...
//        if (false) {
////            val origStageGen = ARQ.getContext().get<Any>(ARQ.stageGenerator) as StageGenerator
//            val stageGenAlt = StageGeneratorAlt()
//            ARQ.getContext().set(ARQ.stageGenerator, stageGenAlt)
//        }
//
//        val query = QueryFactory.create(queryString.joinToString("\n"))
//        val engine = QueryExecutionFactory.create(query, makeData())
//
//        // ... or set on a per-execution basis.
//        if (true) {
////            println(engine.getContext())
////            println(ARQ.getContext())
////            val stageGenAlt = StageGeneratorAlt(engine.getContext().get(ARQ.stageGenerator))
//            val stageGenAlt = StageGeneratorAlt()
//            engine.context.set(ARQ.stageGenerator, stageGenAlt)
//        }
//
//        QueryExecUtils.executeQuery(query, engine)
    }

    private fun loadData(): Model {
        val model = ModelFactory.createDefaultModel()
        RDFDataMgr.read(model, File("models/shelburne.ttl").inputStream(), RDFLanguages.TURTLE)
        return model
    }
}