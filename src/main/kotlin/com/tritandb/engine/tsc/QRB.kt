package com.tritandb.engine.tsc

/**
 * TritanDb
 * Created by eugene on 22/09/2017.
 */
class QRB {
    fun insertionSort(input: IntArray): IntArray {
        var temp: Int
        for (i in 1 until input.size) {
            for (j in i downTo 1) {
                if (input[j] < input[j - 1]) {
                    temp = input[j]
                    input[j] = input[j - 1]
                    input[j - 1] = temp
                }
            }
        }
        return input
    }
}