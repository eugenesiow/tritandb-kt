package com.tritandb.engine.query.engine

import org.apache.jena.atlas.io.IndentedWriter
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.binding.BindingRoot
import org.apache.jena.sparql.engine.iterator.QueryIterYieldN
import org.apache.jena.sparql.serializer.SerializationContext

/**
 * Created by eugenesiow on 21/06/2017.
 */

class QueryIterRootAlt(binding: Binding, execCxt: ExecutionContext) : QueryIterYieldNAlt(1, binding, execCxt) {

    override fun output(out: IndentedWriter, sCxt: SerializationContext?) {
        if (binding is BindingRoot)
            out.print("QueryIterRoot")
        else
            out.print("QueryIterRoot: " + binding)
    }

    companion object {
        fun create(execCxt: ExecutionContext): QueryIterRootAlt {
            return QueryIterRootAlt(BindingRoot.create(), execCxt)
        }

        fun create(binding: Binding, execCxt: ExecutionContext): QueryIterRootAlt {
            return QueryIterRootAlt(binding, execCxt)
        }
    }
}
