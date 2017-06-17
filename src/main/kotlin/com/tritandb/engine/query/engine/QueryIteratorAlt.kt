package com.tritandb.engine.query.engine

import org.apache.jena.graph.Node
import org.apache.jena.graph.Triple
import org.apache.jena.sparql.ARQInternalErrorException
import org.apache.jena.sparql.core.Var
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.binding.BindingFactory
import org.apache.jena.sparql.engine.binding.BindingMap
import org.apache.jena.sparql.engine.iterator.QueryIter
import org.apache.jena.sparql.engine.iterator.QueryIterRepeatApply
import org.apache.jena.util.iterator.ClosableIterator
import org.apache.jena.util.iterator.NiceIterator

/**
 * Created by eugenesiow on 18/06/2017.
 */

class QueryIteratorAlt(input: QueryIterator,
                             private val pattern: Triple,
                             cxt: ExecutionContext) : QueryIterRepeatApply(input, cxt) {

    override fun nextStage(binding: Binding): QueryIterator {
        return TripleMapper(binding, pattern, execContext)
    }

    internal class TripleMapper(private val binding: Binding, pattern: Triple, cxt: ExecutionContext) : QueryIter(cxt) {
        private val s: Node
        private val p: Node
        private val o: Node
        private var graphIter: ClosableIterator<Triple>? = null
        private var slot: Binding? = null
        private var finished = false
        @Volatile private var cancelled = false

        init {
            this.s = substitute(pattern.subject, binding)
            this.p = substitute(pattern.predicate, binding)
            this.o = substitute(pattern.`object`, binding)
            val s2 = tripleNode(s)
            val p2 = tripleNode(p)
            val o2 = tripleNode(o)
            val graph = cxt.activeGraph
            this.graphIter = graph.find(s2, p2, o2)
        }

        private fun tripleNode(node: Node): Node {
            if (node.isVariable)
                return Node.ANY
            return node
        }

        private fun substitute(node: Node, binding: Binding): Node {
            if (Var.isVar(node)) {
                val x = binding.get(Var.alloc(node))
                if (x != null)
                    return x
            }
            return node
        }

        private fun mapper(r: Triple): Binding? {
            val results = BindingFactory.create(binding)

            if (!insert(s, r.subject, results))
                return null
            if (!insert(p, r.predicate, results))
                return null
            if (!insert(o, r.`object`, results))
                return null
            return results
        }

        private fun insert(inputNode: Node, outputNode: Node, results: BindingMap): Boolean {
            if (!Var.isVar(inputNode))
                return true

            val v = Var.alloc(inputNode)
            val x = results.get(v)
            if (x != null)
                return outputNode == x

            results.add(v, outputNode)
            return true
        }

        override fun hasNextBinding(): Boolean {
            if (finished) return false
            if (slot != null) return true
            if (cancelled) {
                graphIter!!.close()
                finished = true
                return false
            }

            while (graphIter!!.hasNext() && slot == null) {
                val t = graphIter!!.next()
                slot = mapper(t)
            }
            if (slot == null)
                finished = true
            return slot != null
        }

        override fun moveToNextBinding(): Binding {
            if (!hasNextBinding())
                throw ARQInternalErrorException()
            val r = slot
            slot = null
            return r!!
        }

        override fun closeIterator() {
            if (graphIter != null)
                NiceIterator.close(graphIter)
            graphIter = null
        }

        override fun requestCancel() {
            // The QueryIteratorBase machinary will do the real work.
            cancelled = true
        }
    }

    companion object {

        internal var countMapper = 0
    }
}