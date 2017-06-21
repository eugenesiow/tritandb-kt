package com.tritandb.engine.query.engine

import org.apache.jena.atlas.io.IndentedWriter
import org.apache.jena.graph.Graph
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.iterator.*
import org.apache.jena.sparql.serializer.SerializationContext

/**
 * Created by eugenesiow on 21/06/2017.
 */
abstract class QueryIterAlt(val graph: Graph): QueryIteratorBase() {
    // Volatile just to make it safe to concurrent updates
    // It does not matter too much if it is wrong - it's used as a label.
    @Volatile internal var iteratorCounter = 0
    private val iteratorNumber = iteratorCounter++

//    private var tracker: ExecutionContext?

    init {
//        tracker = execCxt
//        register()
    }

//    fun makeTracked(qIter: QueryIterator, execCxt: ExecutionContext): QueryIter {
//        if (qIter is QueryIter)
//            return qIter
//        return QueryIterTracked(qIter, execCxt)
//    }

//    fun materialize(qIter: QueryIterator, execCxt: ExecutionContext): QueryIter {
//        return makeTracked(materialize(qIter), execCxt)
//    }
//
//    fun materialize(qIter: QueryIterator): QueryIterator {
//        return QueryIteratorCopy(qIter)
//    }

    fun map(qIter: QueryIterator, varMapping: Map<Var, Var>): QueryIterator {
        return QueryIteratorMapped(qIter, varMapping)
    }

    override fun close() {
        super.close()
//        deregister()
    }

//    fun getExecContext(): ExecutionContext? {
//        return tracker
//    }

    fun getIteratorNumber(): Int {
        return iteratorNumber
    }

    override fun output(out: IndentedWriter) {
        output(out, null)
        //        out.print(Plan.startMarker) ;
        //        out.print(Utils.className(this)) ;
        //        out.print(Plan.finishMarker) ;
    }

    override fun output(out: IndentedWriter, sCxt: SerializationContext?) {
        out.println(getIteratorNumber().toString() + "/" + debug())
    }

//    private fun register() {
//        if (tracker != null)
//            tracker.openIterator(this)
//    }
//
//    private fun deregister() {
//        if (tracker != null)
//            tracker.closedIterator(this)
//    }
}