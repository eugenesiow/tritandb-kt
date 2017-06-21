package com.tritandb.engine.query.engine

import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.algebra.OpVisitor
import org.apache.jena.sparql.algebra.op.*
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.iterator.QueryIterRoot
import org.apache.jena.sparql.engine.main.QC
import org.apache.jena.sparql.util.Context


/**
* Created by eugenesiow on 21/06/2017.
*/
class SparqlOpVisitor: OpVisitor {
    private var model:Model? = null
    private val bgpBindings = mutableListOf<Binding>()

    override fun visit(opBGP: OpBGP?) {
        val context = Context()
        val execCxt = ExecutionContext(context, DatasetFactory.create(model).asDatasetGraph().defaultGraph, null, QC.getFactory(context))
        // Wrap with something to check for closed iterators.
        var qIter:QueryIterator = QueryIterRoot.create(execCxt)
        for (triple in opBGP!!.pattern.list) {
            qIter = QueryIteratorAlt(qIter, triple, execCxt)
        }
        while(qIter.hasNext())
            bgpBindings.add(qIter.next())
    }

    override fun visit(quadPattern: OpQuadPattern?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(quadBlock: OpQuadBlock?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opTriple: OpTriple?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opQuad: OpQuad?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opPath: OpPath?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opTable: OpTable?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opNull: OpNull?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opProc: OpProcedure?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opPropFunc: OpPropFunc?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opFilter: OpFilter?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opGraph: OpGraph?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opService: OpService?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(dsNames: OpDatasetNames?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opLabel: OpLabel?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opAssign: OpAssign?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opExtend: OpExtend?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opJoin: OpJoin?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opLeftJoin: OpLeftJoin?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opUnion: OpUnion?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opDiff: OpDiff?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opMinus: OpMinus?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opCondition: OpConditional?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opSequence: OpSequence?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opDisjunction: OpDisjunction?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opList: OpList?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opOrder: OpOrder?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opProject: OpProject?) {
        for(binding in bgpBindings)
            for(pVar in opProject!!.vars)
                println(binding.get(pVar))
    }

    override fun visit(opReduced: OpReduced?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opDistinct: OpDistinct?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opSlice: OpSlice?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opGroup: OpGroup?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opTop: OpTopN?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun  setModel(model: Model) {
        this.model = model
    }

}