package com.tritandb.engine.query.engine

import com.tritandb.engine.query.op.RangeFlatChunk
import com.tritandb.engine.query.op.TrOp
import com.tritandb.engine.tsc.data.Row
import org.apache.jena.graph.Node
import org.apache.jena.graph.NodeFactory
import org.apache.jena.query.DatasetFactory
import org.apache.jena.rdf.model.Model
import org.apache.jena.sparql.algebra.OpVisitor
import org.apache.jena.sparql.algebra.op.*
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.expr.Expr
import org.apache.jena.sparql.expr.ExprAggregator
import java.text.SimpleDateFormat
import kotlin.coroutines.experimental.buildIterator


/**
* Created by eugenesiow on 21/06/2017.
*/
class SparqlOpVisitor: OpVisitor {
    private var model:Model? = null
    private val bgpBindings = mutableListOf<Binding>()
    private val plan = mutableMapOf<String,TrOp>()
    private val bindingsMap = mutableMapOf<String, MutableMap<String,Node>>()

    override fun visit(opBGP: OpBGP?) {
//        val context = Context()
//        val graph = GraphFactory.createGraphMem()
//        RDFParserBuilder.create().lang(Lang.TTL).fromString(testdata).parse(graph)
//        val execCxt = ExecutionContext(context, DatasetFactory.create(model).asDatasetGraph().defaultGraph, null, null)
        // Wrap with something to check for closed iterators.
        var qIter:QueryIterator = QueryIterRootAlt.create(DatasetFactory.create(model).asDatasetGraph().defaultGraph)
//        var qIter:QueryIterator = QueryIterRootAlt.create(execCxt)
        for (triple in opBGP!!.pattern.list) {
            qIter = QueryIteratorAlt(qIter, triple, DatasetFactory.create(model).asDatasetGraph().defaultGraph)
        }

        val currentBinding = mutableMapOf<String, Node>()
        while(qIter.hasNext()) {
            val binding = qIter.next()
            bgpBindings.add(binding)

            for(v in binding.vars())
                currentBinding.put(v.name, binding.get(v))
        }
        bindingsMap.put(opBGP.toString().trim().hashCode().toString(),currentBinding)
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
        val subOpCode = opFilter!!.subOp.toString().trim().hashCode().toString()
        val newHash = opFilter.toString().trim().hashCode().toString()
        val currentBinding = bindingsMap.remove(subOpCode)
        bindingsMap.put(newHash,currentBinding!!)
        opFilter.exprs.forEach { exp ->
            filterOp(exp, currentBinding)
        }
    }

    fun filterOp(exp: Expr, currentBinding: MutableMap<String, Node>) {
        if(exp.isFunction) {
            when(exp.function.opName) {
                "&&" -> {
                    filterOp(exp.function.args[0], currentBinding)
                    filterOp(exp.function.args[1], currentBinding)
                }
                "<=","<" -> {
                    processRange(exp.function.args[0],exp.function.args[1],false, currentBinding)
                }
                ">=",">" -> {
                    processRange(exp.function.args[0],exp.function.args[1],true, currentBinding)

                }
            }

        }

    }

    private fun  processRange(variable: Expr?, value: Expr?, isStart: Boolean, currentBinding: MutableMap<String, Node>) {
        val sdf = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss")
        val node = currentBinding[variable!!.varName]!!
        if(node.literalDatatypeURI == "http://iot.soton.ac.uk/s2s/s2sml#literalMap") {
            val tsNameParts = node.literalLexicalForm.split(".")
            if(value!!.isConstant) {
                val timestamp = sdf.parse(value.constant.string).time
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
        val subOpCode = opExtend!!.subOp.toString().trim().hashCode().toString()
        val newHash = opExtend.toString().trim().hashCode().toString()
        val currentBinding = bindingsMap.remove(subOpCode)!!
        for((k,v) in opExtend.varExprList.exprs) {
            val value = currentBinding.remove(v.varName)
            currentBinding.put(k.name,value!!)
        }
        bindingsMap.put(newHash, currentBinding)
    }

    override fun visit(opJoin: OpJoin?) {
        val newHash = opJoin.toString().trim().hashCode().toString()
        val leftHash = opJoin!!.left.toString().trim().hashCode().toString()
        val rightHash = opJoin!!.right.toString().trim().hashCode().toString()
        val left = bindingsMap.remove(leftHash)
        val right = bindingsMap.remove(rightHash)
//        println("$leftHash:$left")
//        println("$rightHash:$right")
        left?.plusAssign(right!!)
//        println("$newHash:$left")
        bindingsMap.put(newHash,left!!)
    }

    override fun visit(opLeftJoin: OpLeftJoin?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    override fun visit(opUnion: OpUnion?) {
        val newHash = opUnion.toString().trim().hashCode().toString()
        val leftHash = opUnion!!.left.toString().trim().hashCode().toString()
        val rightHash = opUnion!!.right.toString().trim().hashCode().toString()
        val left = bindingsMap.remove(leftHash)
        val right = bindingsMap.remove(rightHash)
//        println("$leftHash:$left")
//        println("$rightHash:$right")
        left?.plusAssign(right!!)
//        println("$newHash:$left")
        bindingsMap.put(newHash,left!!)
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
        val subOpCode = opProject!!.subOp.toString().trim().hashCode().toString()
        val currentBinding = bindingsMap.remove(subOpCode)!!
//        println(currentBinding)
        for(pVar in opProject.vars) {
            val value= currentBinding[pVar.varName]
            if(value!=null) {
                if (value.isLiteral && value.literalDatatypeURI == "http://iot.soton.ac.uk/s2s/s2sml#literalMap") //binding to timeseries
                    projectCols(value.literalLexicalForm)
                else if(value.isLiteral) { //aggregation
                    //do something
                }
            }
//                println("$binding:$pVar:${binding.get(pVar).isLiteral}")
        }

//        for((_,p) in plan) {
//            p.execute()
//        }
    }

//    fun getIterator():Iterator<Row> {
//        if(plan.isNotEmpty()) {
//            for((_,p) in plan)
//                return (p as RangeFlatChunk).iterator
//        }
//        return  buildIterator {  }
//    }

    fun getPlan(): MutableMap<String, TrOp> {
        return plan
    }

    private fun  projectCols(col:String) {
        val tsNameParts = col.split(".")
        var op: TrOp? = plan.get(tsNameParts[0])
        if(op==null)
            op = RangeFlatChunk("data/shelburne.tsc")
        val opRange =  op as RangeFlatChunk
        opRange.cols.add(tsNameParts[1])
//        println(tsNameParts[1])
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
        val subOpCode = opGroup!!.subOp.toString().trim().hashCode().toString()
        val newHash = opGroup.toString().trim().hashCode().toString()
        val currentBinding = bindingsMap.remove(subOpCode)!!
        opGroup.aggregators.forEach{
            aggr -> aggregation(aggr!!,currentBinding)
        }
        bindingsMap.put(newHash,currentBinding)
    }

    private fun aggregation(aggr: ExprAggregator, currentBinding: MutableMap<String, Node>) {
        for(expr in aggr.aggregator.exprList) {
            for(binding in bgpBindings) {
                val b = binding.get(expr.asVar())
                if(b.isLiteral && b.literalDatatypeURI == "http://iot.soton.ac.uk/s2s/s2sml#literalMap") {
                    processAggr(b.literalLexicalForm,aggr.aggVar.varName,aggr.aggregator.name,currentBinding)
                }
            }
        }
    }

    private fun  processAggr(col: String, varName: String, aggrName: String,currentBinding: MutableMap<String, Node>) {
        val tsNameParts = col.split(".")
        var op: TrOp? = plan.get(tsNameParts[0])
        if(op==null)
            op = RangeFlatChunk("data/shelburne.tsc")
        val opRange =  op as RangeFlatChunk
        opRange.cols.add(tsNameParts[1])
        opRange.aggr(col,varName,aggrName)
        currentBinding.put(varName,NodeFactory.createLiteral("${tsNameParts[0]}.$varName"))
        plan.put(tsNameParts[0],opRange)
    }


    override fun visit(opTop: OpTopN?) {
        TODO("not implemented") //To change body of created functions use File | Settings | File Templates.
    }

    fun  setModel(model: Model) {
        this.model = model
    }

}