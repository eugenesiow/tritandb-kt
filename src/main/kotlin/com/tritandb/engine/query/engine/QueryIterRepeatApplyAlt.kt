package com.tritandb.engine.query.engine

import org.apache.jena.atlas.lib.Lib
import org.apache.jena.atlas.logging.Log
import org.apache.jena.sparql.engine.ExecutionContext
import org.apache.jena.sparql.engine.QueryIterator
import org.apache.jena.sparql.engine.binding.Binding
import org.apache.jena.sparql.engine.iterator.QueryIter1
import org.apache.jena.sparql.engine.iterator.QueryIteratorBase
import java.util.NoSuchElementException

/**
 * Created by eugenesiow on 21/06/2017.
 */

abstract class QueryIterRepeatApplyAlt(input: QueryIterator,
                                    context: ExecutionContext) : QueryIter1Alt(input, context) {
    internal var count = 0
    protected var currentStage: QueryIterator? = null
        private set
    @Volatile private var cancelRequested = false   // [CANCEL] needed? super.cancelRequest?

    init {
        this.currentStage = null

        if (input == null) {
            Log.error(this, "[QueryIterRepeatApply] Repeated application to null input iterator")
//            return
        }
    }

    protected abstract fun nextStage(binding: Binding): QueryIterator

    override fun hasNextBinding(): Boolean {
        if (isFinished)
            return false

        while (true) {
            if (currentStage == null)
                currentStage = makeNextStage()

            if (currentStage == null)
                return false

            if (cancelRequested)
            // Pass on the cancelRequest to the active stage.
                QueryIteratorBase.performRequestCancel(currentStage)

            if (currentStage!!.hasNext())
                return true

            // finish this step
            currentStage!!.close()
            currentStage = null
            // loop
        }
        // Unreachable
    }

    override fun moveToNextBinding(): Binding {
        if (!hasNextBinding())
            throw NoSuchElementException(Lib.className(this) + ".next()/finished")
        return currentStage!!.nextBinding()

    }

    private fun makeNextStage(): QueryIterator? {
        count++

        if (input == null)
            return null

        if (!input!!.hasNext()) {
            input!!.close()
            return null
        }

        val binding = input!!.next()
        val iter = nextStage(binding)
        return iter
    }

    override fun closeSubIterator() {
        if (currentStage != null)
            currentStage!!.close()
    }

    override fun requestSubCancel() {
        if (currentStage != null)
            currentStage!!.cancel() // [CANCEL]
        cancelRequested = true
    }
}