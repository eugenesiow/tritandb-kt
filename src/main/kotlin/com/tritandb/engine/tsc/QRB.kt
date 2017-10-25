package com.tritandb.engine.tsc

import com.tritandb.engine.tsc.data.Row
import java.util.concurrent.ArrayBlockingQueue



/**
 * TritanDb
 * Created by eugene on 22/09/2017.
 */
class QRB(val Q:Int, val QA:Int) {
    private var queue = ArrayBlockingQueue<Row>(Q)
    private var min = 0L
    private var inputCollection = mutableListOf<Row>()

    fun insert(r:Row) {
        queue.put(r)
        if(queue.size>=Q) {
            //flush queue and sort
            synchronized(queue) {
                queue.drainTo(inputCollection)
            }
            val sortedArr = insertionSort(inputCollection.toTypedArray())
            val remainingArr = sortedArr.sliceArray(QA..(sortedArr.size-1))
            val flushedArr = sortedArr.sliceArray(0..(QA-1))
            //set qrb min val to min (first val) of remaining array
            synchronized(queue) {
                queue.addAll(remainingArr) //add remainder back
            }
            //flush the sorted arr to memtable
//            memtable.addAll(flushedArr)
//            memtable.indexUpdate()
        }
    }

    private fun insertionSort(input: Array<Row>): Array<Row> {
        var temp: Row
        for (i in 1 until input.size) {
            for (j in i downTo 1) {
                if (input[j].timestamp < input[j - 1].timestamp) {
                    temp = input[j]
                    input[j] = input[j - 1]
                    input[j - 1] = temp
                }
            }
        }
        return input
    }
}