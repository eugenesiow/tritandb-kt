package com.tritandb.engine.query.engine

import org.apache.jena.atlas.io.IndentedWriter
import org.apache.jena.atlas.lib.Lib
import org.apache.jena.graph.Graph
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.serializer.SerializationContext
import java.util.*

/**
 * Created by eugenesiow on 21/06/2017.
 */

open class QueryIterYieldNAlt(protected var limitYielded: Int, b: Binding, g: Graph) : QueryIterAlt(g) {
    protected var countYielded = 0
    var binding: Binding
        protected set

    init {
        binding = b
    }

    override fun hasNextBinding(): Boolean {
        return countYielded < limitYielded
    }

    override fun moveToNextBinding(): Binding {
        if (!hasNextBinding())
        // Try to get the class name as specific as possible for subclasses
            throw NoSuchElementException(Lib.className(this))
        countYielded++
        return binding
    }

    override fun closeIterator() {
        //binding = null ;
    }

    override fun requestCancel() {}

    override fun output(out: IndentedWriter, sCxt: SerializationContext?) {
        out.print("QueryIterYieldN: $limitYielded of $binding")
    }

}