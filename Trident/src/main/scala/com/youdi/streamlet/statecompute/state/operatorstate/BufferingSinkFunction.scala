package com.youdi.streamlet.statecompute.state.operatorstate

import java.util

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.api.common.typeinfo.{TypeHint, TypeInformation}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.sink.SinkFunction

class BufferingSinkFunction extends SinkFunction[String, String] with CheckpointedFunction {
  private var threshold: Int = _
  @transient private var checkpointedState: ListState[(String, String)] = _
  private var bufferElements: List[Tuple2[String, String]] = _

  def this(threshold: Int) {
    this
    this.threshold = threshold
    this.bufferElements = List()
  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkpointedState.clear()
    for (ele <- bufferElements) {
      // sink
    }
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    checkpointedState = context.getOperatorStateStore.getListState(
      //      new ListStateDescriptor("buffered-elements", classOf[Tuple2[String, String]])
      new ListStateDescriptor("buffered-elements", TypeInformation.of(new TypeHint[Tuple2[String, String]] {}))
    )

    if (context.isRestored) {
      val iterator: util.Iterator[Tuple2[String, String]] = checkpointedState.get.iterator()
      while (iterator.hasNext) {
        val tuple: Tuple2[String, String] = iterator.next()

      }
    }

  }

  override def invoke(value: String, context: SinkFunction.Context): Unit = {
    bufferElements ++ value
    if (bufferElements.length == threshold) {
      for (ele <- bufferElements) {
        // sink
      }
    }
    bufferElements = List()
  }
}
