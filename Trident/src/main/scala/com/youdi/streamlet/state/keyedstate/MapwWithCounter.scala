package com.youdi.streamlet.state.keyedstate

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.api.common.state.{ValueState, ValueStateDescriptor}
import org.apache.flink.configuration.Configuration

class MapwWithCounter extends RichMapFunction[String, String] {
  private var totalLenthBytes: ValueState[Long] = _


  override def open(parameters: Configuration): Unit = {
    super.open(parameters) // 调用父类的open方法
    totalLenthBytes = getRuntimeContext.getState(new ValueStateDescriptor[Long]("sum of length", classOf[Long]))
  }


  // 一直累积长度
  override def map(value: String): Long = {
    var l: Long = totalLenthBytes.value()
    if (null == l) {
      l = 0L
    }
    val l1: Long = l + value.length

    totalLenthBytes.update(l1)
    l1
  }
}
