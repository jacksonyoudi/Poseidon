package com.youdi.streamlet.sideout

import org.apache.flink.streaming.api.functions.ProcessFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector

object DTag {
  def main(args: Array[String]): Unit = {
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    val DS: DataStream[String] = env.socketTextStream("localhost", 9999)

    val ds: DataStream[Long] = DS.flatMap(_.split(" ")).map(_.toLong)

    val tag: OutputTag[String] = new OutputTag[String]("side")

    val dataDS: DataStream[Long] = ds.process(new ProcessFunction[Long, Long]() {
      override def processElement(value: Long, ctx: ProcessFunction[Long, Long]#Context, out: Collector[Long]): Unit = {
        out.collect(value)
        ctx.output(tag, "sideout-" + value)
      }
    })


    dataDS.getSideOutput(tag)


    env.execute()

  }
}
