package com.tritandb.engine.tsc.data

import com.tritandb.engine.tsc.data.*
import com.tritandb.engine.tsc.data.EventProtos
import com.tritandb.engine.tsc.data.EventProtos.RowOrBuilder
import com.tritandb.engine.tsc.data.EventProtos.Row
import com.tritandb.engine.tsc.data.EventProtos.Row.Builder
import com.tritandb.engine.tsc.data.EventProtos.RowsOrBuilder
import com.tritandb.engine.tsc.data.EventProtos.Rows
import com.tritandb.engine.tsc.data.EventProtos.ColOrBuilder
import com.tritandb.engine.tsc.data.EventProtos.Col
import com.tritandb.engine.tsc.data.EventProtos.Col.ColType
import com.tritandb.engine.tsc.data.EventProtos.ColsOrBuilder
import com.tritandb.engine.tsc.data.EventProtos.Cols
import com.tritandb.engine.tsc.data.EventProtos.TritanEventOrBuilder
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent
import com.tritandb.engine.tsc.data.EventProtos.TritanEvent.EventType

public inline fun buildRow(fn: Row.Builder.() -> Unit): Row {
    val builder = Row.newBuilder()
    builder.fn()
    return builder.build()
}
public inline fun buildRows(fn: Rows.Builder.() -> Unit): Rows {
    val builder = Rows.newBuilder()
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
