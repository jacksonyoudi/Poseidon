package com.youdi.streamlet.statecompute.processfunction

import org.apache.flink.api.common.state.StateTtlConfig.UpdateType
import org.apache.flink.api.common.state.{StateTtlConfig, ValueState, ValueStateDescriptor}
import org.apache.flink.api.common.time.Time
import org.apache.flink.configuration.Configuration
import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.util.Collector


class CountWithTimeoutFunction extends ProcessFunction[Tuple2[String, String], Tuple2[String, Long]] {
  var state: ValueState[CountWithTimestamp] = null

  // 处理每条消息
  override def processElement(value: (String, String), ctx: ProcessFunction[(String, String), (String, Long)]#Context, out: Collector[(String, Long)]): Unit = {
    var current: CountWithTimestamp = state.value()
    if (current != null) {
      current = new CountWithTimestamp(value._1)
    }

    current.count += 1
    current.lastModified = ctx.timestamp()
    state.update(current)

    ctx.timerService().registerEventTimeTimer(current.lastModified + 100)
  }

  // 定时器
  override def onTimer(timestamp: Long, ctx: ProcessFunction[(String, String), (String, Long)]#OnTimerContext, out: Collector[(String, Long)]): Unit = {
    val result: CountWithTimestamp = state.value()

    if (timestamp == result.lastModified + 100) {
      out.collect(new Tuple2[String, Long](result.key, result.count.toLong))
      state.clear()
    }
  }

  // 初始化 状态
  override def open(parameters: Configuration): Unit = {
    val desc: ValueStateDescriptor[CountWithTimestamp] = new ValueStateDescriptor[CountWithTimestamp]("state", classOf[CountWithTimestamp])

    val ttlConfig: StateTtlConfig = new StateTtlConfig.Builder(Time.hours(24))
      .setUpdateType(UpdateType.OnCreateAndWrite)
      .build()
    desc.enableTimeToLive(ttlConfig)
    state = getRuntimeContext.getState[CountWithTimestamp](desc)
  }
}
