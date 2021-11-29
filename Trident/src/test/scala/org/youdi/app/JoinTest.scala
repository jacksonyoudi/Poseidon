package org.youdi.app

import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.util.Collector
import org.youdi.bean.{One, Two}


object JoinTest {
  def main(args: Array[String]): Unit = {
    // 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    // 读取nc数据并提取时间时间戳生成watermark
    val oneDS: SingleOutputStreamOperator[One] = env
      .socketTextStream("localhost", 9998)
      .map(lines => {
        val words: Array[String] = lines.split(",")
        new One(words(0), words(1), words(2).toLong)
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[One]().withTimestampAssigner(
        new SerializableTimestampAssigner[One]() {
          override def extractTimestamp(element: One, recordTimestamp: Long) = {
            element.ts * 1000
          }
        }
      )
    )

    val twoDS: SingleOutputStreamOperator[Two] = env
      .socketTextStream("localhost", 9997)
      .map(lines => {
        val words: Array[String] = lines.split(",")
        new Two(words(0), words(1), words(2).toLong)
      }).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[Two]().withTimestampAssigner(
        new SerializableTimestampAssigner[Two]() {
          override def extractTimestamp(element: Two, recordTimestamp: Long) = {
            element.ts * 1000
          }
        }
      )
    )

    val joinDS: SingleOutputStreamOperator[(One, Two)] = oneDS.keyBy(new KeySelector[One, String]() {
      override def getKey(value: One): String = {
        value.id
      }
    }).intervalJoin(
      twoDS.keyBy(new KeySelector[Two, String]() {
        override def getKey(value: Two) = {
          value.id
        }
      })
    ).between(Time.seconds(-5),
      Time.seconds(5)
    )
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(
        new ProcessJoinFunction[One, Two, Tuple2[One, Two]]() {
          override def processElement(left: One, right: Two, ctx: ProcessJoinFunction[One, Two, Tuple2[One, Two]]#Context, out: Collector[Tuple2[One, Two]]): Unit = {
            out.collect(
              new Tuple2[One, Two](left, right)
            )
          }
        }
      )
    // 双流jion

    joinDS.print("join")

    // 打印
    env.execute("start")

  }
}



