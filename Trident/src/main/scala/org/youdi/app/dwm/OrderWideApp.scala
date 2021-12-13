package org.youdi.app.dwm

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkStrategy}
import org.apache.flink.streaming.api.windowing.time.Time
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.functions.co.ProcessJoinFunction
import org.apache.flink.streaming.api.scala.{DataStream, StreamExecutionEnvironment}
import org.youdi.utils.KafkaUtils
import org.apache.flink.streaming.api.scala._
import org.apache.flink.util.Collector
import org.youdi.app.function.DimAsyncFucntion
import org.youdi.bean.{OrderDetail, OrderInfo, OrderWide}

import java.text.SimpleDateFormat
import java.util.concurrent.TimeUnit

object OrderWideApp {
  def main(args: Array[String]): Unit = {
    // 1. 执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 和kafka分区保持一致

    // 开启checkpointing

    env.setStateBackend(new RocksDBStateBackend("file:///opt/module/applog/gmall2020/OrderWideApp/"))

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3))

    // 2. 读取kafka数据 3. 将每行数据转换成json对象


    val orderInfoScourceTopic: String = "dwd_order_info"
    val orderDetailScourceTopic: String = "dwd_order_detail"
    val orderWideSinkTopic: String = "dwm_order_wide"
    val groupid: String = "order_wide_group"

    // 2. 读取kafka主题数据 并转换为 bean对象， 提取时间戳生成watermark
    val orderInfoDS: DataStream[OrderInfo] = env
      .addSource(KafkaUtils.getKafkaConsumer(orderInfoScourceTopic, groupid))
      .map(line => {
        val info: OrderInfo = JSON.parseObject(line, classOf[OrderInfo])
        val create_time: String = info.create_time
        val dateTimeArr: Array[String] = create_time.split(" ")
        info.create_date = dateTimeArr(0)
        info.create_hour = dateTimeArr(1).split(":")(0)

        val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
        info.create_ts = format.parse(create_time).getTime
        info
      }
      ).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[OrderInfo]()
        .withTimestampAssigner(
          new SerializableTimestampAssigner[OrderInfo]() {
            override def extractTimestamp(element: OrderInfo, recordTimestamp: Long) = {
              element.create_ts
            }
          }
        )
    )


    val orderDetailDS: DataStream[OrderDetail] = env.addSource(KafkaUtils.getKafkaConsumer(orderDetailScourceTopic, groupid))
      .map(
        line => {
          val detail: OrderDetail = JSON.parseObject(line, classOf[OrderDetail])

          val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")
          detail.create_ts = format.parse(detail.create_time).getTime

          detail
        }
      ).assignTimestampsAndWatermarks(
      WatermarkStrategy.forMonotonousTimestamps[OrderDetail]()
        .withTimestampAssigner(
          new SerializableTimestampAssigner[OrderDetail]() {
            override def extractTimestamp(element: OrderDetail, recordTimestamp: Long) = {
              element.create_ts
            }
          }
        )
    )
    val wideNoDimDS: DataStream[OrderWide] = orderInfoDS.keyBy(new KeySelector[OrderInfo, Long]() {
      override def getKey(in: OrderInfo) = {
        in.id
      }
    }).intervalJoin(
      orderDetailDS.keyBy(new KeySelector[OrderDetail, Long]() {
        override def getKey(in: OrderDetail) = {
          in.order_id
        }
      })
    ).between(Time.seconds(-5), Time.seconds(5)) // 生产环境中给的时间是最大延迟时间
      .lowerBoundExclusive()
      .upperBoundExclusive()
      .process(
        new ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide]() {
          override def processElement(in1: OrderInfo, in2: OrderDetail, context: ProcessJoinFunction[OrderInfo, OrderDetail, OrderWide]#Context, collector: Collector[OrderWide]): Unit = {
            collector.collect(new OrderWide(in1, in2))
          }
        }
      )

    // 关联维度信息

    // 关联用户维度
    val withUserDS: DataStream[OrderWide] = AsyncDataStream.unorderedWait(
      wideNoDimDS,
      new DimAsyncFucntion[OrderWide]("DIM_USER_INFO") {
        override def getKey(input: OrderWide) = {
          input.user_id.toString
        }

        override def join(intput: OrderWide, dimInfo: JSONObject): Unit = {
          intput.user_gender = dimInfo.getString("GENDER")
          val birthday: String = dimInfo.getString("BIRTHDAY")
          val format: SimpleDateFormat = new SimpleDateFormat("yyyy-MM-dd")

          val l: Long = System.currentTimeMillis()
          val time: Long = format.parse(birthday).getTime

          val age: Long = (time - l) / (1000 * 60 * 60 * 24 * 465L)
          intput.user_age = age.toInt

        }
      },
      60,
      TimeUnit.SECONDS
    )
    




    // 数据写回kafka


    //  开启任务


  }

}
