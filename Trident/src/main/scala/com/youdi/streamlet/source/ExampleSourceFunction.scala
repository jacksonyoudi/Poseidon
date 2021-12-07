package com.youdi.streamlet.source

import org.apache.flink.streaming.api.functions.source.SourceFunction

class ExampleSourceFunction[T] extends SourceFunction[T] {
  override def run(ctx: SourceFunction.SourceContext[T]): Unit = {}

  override def cancel(): Unit = {}
}
