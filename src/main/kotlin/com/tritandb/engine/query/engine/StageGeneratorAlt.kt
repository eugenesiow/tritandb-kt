package com.tritandb.engine.query.engine

import org.apache.jena.sparql.core.BasicPattern
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.main.StageGenerator


/**
 * Created by eugenesiow on 18/06/2017.
 */
class StageGeneratorAlt:StageGenerator {

    override fun execute(pattern: BasicPattern,
                         input: QueryIterator,
                         execCxt: ExecutionContext): QueryIterator {
//        // Just want to pick out some BGPs (e.g. on a particualr graph)
//        // Test ::  execCxt.getActiveGraph()
//        if (execCxt.getActiveGraph() !is GraphBase)
//        // Example: pass on up to the original StageGenerator if
//        // not based on GraphBase (which most Graph implementations are).
//            return other.execute(pattern, input, execCxt)

//        System.err.println("MyStageGenerator.compile:: triple patterns = " + pattern.size())

        // Stream the triple matches together, one triple matcher at a time.
        var qIter = input
//        println("before:$qIter")
        for (triple in pattern.list) {
            qIter = QueryIteratorAlt(qIter, triple, execCxt)
        }
        println("after:$qIter:after:${execCxt.activeGraph}")
        println("$qIter:${qIter.next().get(Var.alloc("v"))}")
        return qIter
    }
}