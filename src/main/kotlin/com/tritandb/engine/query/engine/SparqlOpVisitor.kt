package com.tritandb.engine.query.engine

import com.tritandb.engine.query.op.RangeFlat
import com.tritandb.engine.query.op.RangeFlatChunk
import com.tritandb.engine.query.op.TrOp
import com.tritandb.engine.tsc.data.EventProtos
import com.tritandb.engine.tsc.data.Row
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.algebra.OpVisitor
import org.apache.jena.sparql.algebra.op.*
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.expr.Expr
import org.apache.jena.sparql.util.Context
import java.text.SimpleDateFormat
import kotlin.coroutines.experimental.buildIterator


/**
* Created by eugenesiow on 21/06/2017.
*/
class SparqlOpVisitor: OpVisitor {
    private var model:Model? = null
    private val bgpBindings = mutableListOf<Binding>()
    private val plan = mutableMapOf<String,TrOp>()

    override fun visit(opBGP: OpBGP?) {
        val context = Context()
//        val graph = GraphFactory.createGraphMem()
//        RDFParserBuilder.create().lang(Lang.TTL).fromString(testdata).parse(graph)
//        val execCxt = ExecutionContext(context, DatasetFactory.create(model).asDatasetGraph().defaultGraph, null, null)
        // Wrap with something to check for closed iterators.
        var qIter:QueryIterator = QueryIterRootAlt.create(DatasetFactory.create(model).asDatasetGraph().defaultGraph)
//        var qIter:QueryIterator = QueryIterRootAlt.create(execCxt)
        for (triple in opBGP!!.pattern.list) {
            qIter = QueryIteratorAlt(qIter, triple, DatasetFactory.create(model).asDatasetGraph().defaultGraph)
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
        opFilter!!.exprs.forEach { exp ->
            filterOp(exp)
        }
    }

    fun filterOp(exp:Expr) {
        if(exp.isFunction) {
            when(exp.function.opName) {
                "&&" -> {
                    filterOp(exp.function.args[0])
                    filterOp(exp.function.args[1])
                }
                "<=","<" -> {
                    processRange(exp.function.args[0],exp.function.args[1],false)
                }
                ">=",">" -> {
                    processRange(exp.function.args[0],exp.function.args[1],true)

                }
            }

        }

    }

    private fun  processRange(variable: Expr?, value: Expr?, isStart: Boolean) {
        val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        bgpBindings.map { it.get(variable!!.asVar()) }
                .forEach {
                    if(it.literalDatatypeURI == "http://iot.soton.ac.uk/s2s/s2sml#literalMap") {
                        val tsNameParts = it.literalLexicalForm.split(".")
                        if(value!!.isConstant) {
                            val timestamp = sdf.parse(value!!.constant.string).time
                            var op: TrOp? = plan.get(tsNameParts[0])
                            if(op==null)
                                op = RangeFlatChunk("data/shelburne.tsc")
                            val opRange =  op as RangeFlatChunk
                            if (isStart)
                                opRange.start = timestamp
                            else
                                opRange.end = timestamp
                            plan.put(tsNameParts[0],opRange)
                        }
                    }
                }
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
        println(opExtend)
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
        for(pVar in opProject!!.vars) {
            for(binding in bgpBindings) {
                if(binding.get(pVar).isLiteral && binding.get(pVar).literalDatatypeURI == "http://iot.soton.ac.uk/s2s/s2sml#literalMap")
                   projectCols(binding.get(pVar).literalLexicalForm)
//                println("$binding:$pVar:${binding.get(pVar).isLiteral}")
            }
        }

        for((_,p) in plan) {
            p.execute()
        }
    }

    fun getIterator():Iterator<Row> {
        if(plan.isNotEmpty()) {
            for((_,p) in plan)
                return (p as RangeFlatChunk).iterator
        }
        return  buildIterator {  }
    }

    private fun  projectCols(col:String) {
        val tsNameParts = col.split(".")
        var op: TrOp? = plan.get(tsNameParts[0])
        if(op==null)
            op = RangeFlatChunk("data/shelburne.tsc")
        val opRange =  op as RangeFlatChunk
        opRange.cols.add(tsNameParts[1])
        plan.put(tsNameParts[0],opRange)
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
        opGroup!!.aggregators.forEach{
            aggr ->
            println(aggr.aggVar)
            println(aggr.aggregator.name)
            println(aggr.aggregator.exprList.get(0))
            
        }
    }

    override fun visit(opTop: OpTopN?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun  setModel(model: Model) {
        this.model = model
    }

}