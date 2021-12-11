package com.youdi.streamlet.statecompute.state.operatorstate

import java.util

import org.apache.flink.api.common.functions.{MapFunction, ReduceFunction}
import org.apache.flink.api.common.state.{ListState, ListStateDescriptor, ReducingState, ReducingStateDescriptor}
import org.apache.flink.runtime.state.{FunctionInitializationContext, FunctionSnapshotContext}
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction

class CustomMapFunction[T] extends MapFunction[T, T] with CheckpointedFunction {
  private var countPerKey: ReducingState[Long] = _
  private var countPerPartition: ListState[Long] = _

  private var localCount: Long = _

  override def map(value: T): T = {
    countPerKey.add(1L)
    localCount += 1
    value
  }

  // 持久化状态
  override def snapshotState(context: FunctionSnapshotContext): Unit = {
    countPerKey.clear()
    countPerPartition.add(localCount)
  }

  // 初始化状态
  override def initializeState(context: FunctionInitializationContext): Unit = {
    countPerKey = context.getKeyedStateStore.getReducingState(
      new ReducingStateDescriptor[Long]("perkeyCount", new AddFunction, classOf[Long])
    )

    countPerPartition = context.getOperatorStateStore.getUnionListState[Long](new ListStateDescriptor[Long](
      "countPerPartition", classOf[Long]
    ))

    val iterator: util.Iterator[Long] = countPerPartition.get.iterator

    while (iterator.hasNext) {
      val l: Long = iterator.next()
      localCount += l
    }

  }
}

class AddFunction extends ReduceFunction[Long] {
  override def reduce(value1: Long, value2: Long): Long = {
    value1 + value2
  }
}
