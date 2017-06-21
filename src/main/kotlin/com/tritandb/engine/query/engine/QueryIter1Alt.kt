package com.tritandb.engine.query.engine

import org.apache.jena.atlas.io.IndentedWriter
import org.apache.jena.atlas.lib.Lib
import org.apache.jena.graph.Graph
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.iterator.QueryIter
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase
import org.apache.jena.sparql.serializer.SerializationContext

/**
 * Created by eugenesiow on 21/06/2017.
 */
abstract class QueryIter1Alt(input: QueryIterator, g: Graph) : QueryIterAlt(g) {
    protected var input: QueryIterator? = null
        private set

    init {
        this.input = input
    }

    override fun closeIterator() {
        closeSubIterator()
        QueryIteratorBase.performClose(input)
        input = null
    }

    override fun requestCancel() {
        requestSubCancel()
        QueryIteratorBase.performRequestCancel(input)
    }

    /** Cancellation of the query execution is happening  */
    protected abstract fun requestSubCancel()

    /** Pass on the close method - no need to close the QueryIterator passed to the QueryIter1 constructor  */
    protected abstract fun closeSubIterator()

    // Do better
    override fun output(out: IndentedWriter, sCxt: SerializationContext?) {
        // Linear form.
        input!!.output(out, sCxt)
        out.ensureStartOfLine()
        details(out, sCxt)
        out.ensureStartOfLine()

        //        details(out, sCxt) ;
        //        out.ensureStartOfLine() ;
        //        out.incIndent() ;
        //        getInput().output(out, sCxt) ;
        //        out.decIndent() ;
        //        out.ensureStartOfLine() ;
    }

    protected open fun details(out: IndentedWriter, sCxt: SerializationContext?) {
        out.println(Lib.className(this))
    }

}
