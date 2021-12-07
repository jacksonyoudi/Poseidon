package com.youdi.streamlet.source

import org.apache.flink.api.common.state.{ListState, ListStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.SourceFunction

import java.util

class ExampleSourceFunction extends SourceFunction[Long] with CheckpointedFunction {
  var count: Long = 0

  @volatile var isRunning: Boolean = false

  @transient var checkpointCount: ListState[Long] = _


  override def run(ctx: SourceFunction.SourceContext[Long]): Unit = {
    while (isRunning && count < 1000) {
      ctx.getCheckpointLock.synchronized {
        ctx.collect(count)
        count += 1
      }
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }

  override def initializeState(context: FunctionInitializationContext): Unit = {
    this.checkpointCount = context
      .getOperatorStateStore
      .getListState(new ListStateDescriptor[Long]("count", classOf[Long]))

    if (context.isRestored) {
      val itor: util.Iterator[Long] = this.checkpointCount.get().iterator()

      while (itor.hasNext) {
        this.count = itor.next()
      }

    }

  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    checkpointCount.clear()
    checkpointCount.add(count)
  }
}
