package com.youdi.streamlet.datastream.source

import org.apache.flink.configuration.Configuration
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import com.sproutsocial.nsq.Subscriber

class NsqSource() extends RichSourceFunction with CheckpointedFunction {
  var isRunning: Boolean = _
  var subscriber: Subscriber = _
  // 初始化 nsq
  override def open(parameters: Configuration): Unit = {
    super.open(parameters)


  }

  override def snapshotState(context: FunctionSnapshotContext): Unit = {

  }

  override def initializeState(context: FunctionInitializationContext): Unit = {

  }

  override def run(ctx: SourceFunction.SourceContext[Nothing]): Unit = {
    while (isRunning) {
    }
  }

  override def cancel(): Unit = {
    isRunning = false
  }
}
