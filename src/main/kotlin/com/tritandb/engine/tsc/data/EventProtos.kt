package com.tritandb.engine.tsc.data

import com.tritandb.engine.tsc.data.EventProtos.*

public inline fun buildRow(fn: EventProtos.Row.Builder.() -> Unit): EventProtos.Row {
    val builder = EventProtos.Row.newBuilder()
    builder.fn()
    return builder.build()
}
public inline fun buildRows(fn: EventProtos.Rows.Builder.() -> Unit): EventProtos.Rows {
    val builder = EventProtos.Rows.newBuilder()
    builder.fn()
    return builder.build()
}
public inline fun buildCol(fn: Col.Builder.() -> Unit): Col {
    val builder = Col.newBuilder()
    builder.fn()
    return builder.build()
}
public inline fun buildCols(fn: Cols.Builder.() -> Unit): Cols {
    val builder = Cols.newBuilder()
    builder.fn()
    return builder.build()
}
public inline fun buildTritanEvent(fn: TritanEvent.Builder.() -> Unit): TritanEvent {
    val builder = TritanEvent.newBuilder()
    builder.fn()
    return builder.build()
}
