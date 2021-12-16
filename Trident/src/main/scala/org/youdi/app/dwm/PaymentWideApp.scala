package org.youdi.app.dwm

import com.alibaba.fastjson.JSON
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.util.Collector
import org.youdi.bean.{OrderWide, PaymentInfo, PaymentWide}
import org.youdi.utils.KafkaUtils

import java.text.SimpleDateFormat


object PaymentWideApp {
  def main(args: Array[String]): Unit = {
    // 1. 执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 和kafka分区保持一致

    // 开启checkpointing

    env.setStateBackend(new RocksDBStateBackend("file:///opt/module/applog/gmall2020/PaymentWideApp/"))

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3))

    // 2. 读取kafka数据
    val groupId: String = "payment_wide_group"
    val paymentInfoSourceTopic: String = "dwd_payment_info"
    val orderWideSourceTopic: String = "dwm_order_wide"
    val paymentWideSinkTopic: String = "dwm_payment_wide"

    // 2. 读取kafka主题数据创建流， 并转换 javabean， 提取生成watermark
    val sourceDS: DataStream[String] = env.addSource(KafkaUtils.getKafkaConsumer(orderWideSourceTopic, groupId))

    val orderWideSource: DataStream[OrderWide] = sourceDS.map(
      line => {
        JSON.parseObject(line, classOf[OrderWide])
      }
    ).assignTimestampsAndWatermarks(WatermarkStrategy.forMonotonousTimestamps().withTimestampAssigner(
      new SerializableTimestampAssigner[OrderWide]() {
        override def extractTimestamp(element: OrderWide, recordTimestamp: Long) = {
          val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          try {
            format.parse(element.create_time).getTime
          } catch {
            case e: Exception => {
              e.printStackTrace()
              recordTimestamp
            }
          }
        }
      }
    ))


    val paymentInfoDS: DataStream[PaymentInfo] = env.addSource(KafkaUtils.getKafkaConsumer(paymentInfoSourceTopic, groupId))
      .map(line => JSON.parseObject(line, classOf[PaymentInfo]))
      .assignTimestampsAndWatermarks(
        WatermarkStrategy.forMonotonousTimestamps()
          .withTimestampAssigner(new SerializableTimestampAssigner[PaymentInfo]() {
            override def extractTimestamp(element: PaymentInfo, recordTimestamp: Long) = {
              val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
              try {
                format.parse(element.create_time).getTime
              } catch {
                case e: Exception => {
                  e.printStackTrace()
                  recordTimestamp
                }
              }
            }
          })
      )

    // 3. 双流join
    val resultDS: DataStream[PaymentWide] = paymentInfoDS.keyBy(new KeySelector[PaymentInfo, Long]() {
      override def getKey(value: PaymentInfo) = {
        value.order_id
      }
    }).intervalJoin(
      orderWideSource.keyBy(
        new KeySelector[OrderWide, Long]() {
          override def getKey(value: OrderWide) = {
            value.order_id
          }
        }
      )
    ).between(
      Time.minutes(-15), Time.seconds(100)
    ).lowerBoundExclusive()
      .upperBoundExclusive()
      .process(
        new ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide]() {
          override def processElement(left: PaymentInfo, right: OrderWide, ctx: ProcessJoinFunction[PaymentInfo, OrderWide, PaymentWide]#Context, out: Collector[PaymentWide]) = {
            out.collect(new PaymentWide(
              left, right
            ))
          }
        }
      )

    // 4. 将数据写到 kafka

    resultDS.map(obj => JSON.toJSONString(obj)).addSink(KafkaUtils.getKafkaProducer(paymentWideSinkTopic))

    // 5. 启动程序
    env.execute("PaymentWideApp")

  }
}
