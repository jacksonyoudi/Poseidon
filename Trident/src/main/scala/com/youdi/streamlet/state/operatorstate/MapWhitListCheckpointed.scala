package com.youdi.streamlet.state.operatorstate

import org.apache.flink.api.common.functions.MapFunction
import org.apache.flink.streaming.api.checkpoint.ListCheckpointed

import java.util
import java.util.Collections

class MapWhitListCheckpointed[T] extends MapFunction[T, Tuple2[T, Long]] with ListCheckpointed[T] {
  private var count: Long = _


  // 将一个单值返回，会自动
  override def snapshotState(checkpointId: Long, timestamp: Long): util.List[T] = {
    Collections.singletonList(count)
  }

  // 将数据
  override def restoreState(state: util.List[T]): Unit = {
    for (_ <- state.toArray) {
      count += 1
    }
  }

  override def map(value: T): (T, Long) = {
    count += 1
    Tuple2(value, count)
  }
}
