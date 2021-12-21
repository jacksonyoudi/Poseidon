package org.youdi.app.dws

import com.alibaba.fastjson.{JSON, JSONObject}
import org.apache.flink.api.common.eventtime.{SerializableTimestampAssigner, WatermarkGenerator, WatermarkStrategy}
import org.apache.flink.api.java.functions.KeySelector
import org.apache.flink.contrib.streaming.state.RocksDBStateBackend
import org.apache.flink.streaming.api.CheckpointingMode
import org.apache.flink.streaming.api.scala._
import org.apache.flink.streaming.api.windowing.assigners.TumblingEventTimeWindows
import org.apache.flink.streaming.api.windowing.time.Time
import org.youdi.bean.VisitorStats
import org.youdi.utils.KafkaUtils

import java.time.Duration

object VisitorStatsApp {
  def main(args: Array[String]): Unit = {
    // 1. 获取执行环境
    val env: StreamExecutionEnvironment = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1) // 和kafka分区保持一致

    // 开启checkpointing

    env.setStateBackend(new RocksDBStateBackend("file:///opt/module/applog/gmall2020/VisitorStatsApp/"))

    env.enableCheckpointing(5000)
    env.getCheckpointConfig.setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE)
    env.getCheckpointConfig.setCheckpointTimeout(10000L)
    env.getCheckpointConfig.setMaxConcurrentCheckpoints(2)
    env.getCheckpointConfig.setMinPauseBetweenCheckpoints(3000)
    //    env.setRestartStrategy(RestartStrategies.fixedDelayRestart(3))


    // 2. 读取kafka数据创建流
    val pageViewSourceTopic: String = "dwd_page_log"
    val uniqueVisitSourceTopic: String = "dwd_unique_visit"
    val userJumpDetailSourceTopic: String = "dwm_order_wide"
    val groupid: String = "visitor_stats_app"

    val uvDS: DataStream[String] = env.addSource(KafkaUtils.getKafkaConsumer(uniqueVisitSourceTopic, groupid))
    val ujDS: DataStream[String] = env.addSource(KafkaUtils.getKafkaConsumer(userJumpDetailSourceTopic, groupid))
    val pvDS: DataStream[String] = env.addSource(KafkaUtils.getKafkaConsumer(pageViewSourceTopic, groupid))


    // 3. 将每个流处理成相同的数据类型
    val DSwithUV: DataStream[VisitorStats] = uvDS.map(line => {
      val js: JSONObject = JSON.parseObject(line)

      // 提取公共字段
      val common: JSONObject = js.getJSONObject("common")

      VisitorStats(
        "",
        "",
        common.getString("vc"),
        common.getString("ch"),
        common.getString("ar"),
        common.getString("is_new"),
        1L,
        0L,
        0L,
        0L,
        js.getLong("ts")
      )
    }
    )


    val DSwithUJ: DataStream[VisitorStats] = ujDS.map(
      line => {
        val js: JSONObject = JSON.parseObject(line)

        // 提取公共字段
        val common: JSONObject = js.getJSONObject("common")

        VisitorStats(
          "",
          "",
          common.getString("vc"),
          common.getString("ch"),
          common.getString("ar"),
          common.getString("is_new"),
          1L,
          0L,
          0L,
          0L,
          js.getLong("ts")
        )
      }
    )


    val DSwithPV: DataStream[VisitorStats] = pvDS.map(line => {
      val js: JSONObject = JSON.parseObject(line)
      val common: JSONObject = js.getJSONObject("common")
      val page: JSONObject = js.getJSONObject("page")

      // 获取上一跳页面
      val lastPageId: String = page.getString("last_page_id")
      var sv: Long = 0L
      if (lastPageId == null && lastPageId.length <= 0L) {
        sv = 1L
      }


      VisitorStats(
        "",
        "",
        common.getString("vc"),
        common.getString("ch"),
        common.getString("ar"),
        common.getString("is_new"),
        1L,
        0L,
        sv,
        page.getLong("during_time"),
        js.getLong("ts")
      )
    })

    // 4. union 几个流
    val ds: DataStream[VisitorStats] = DSwithUV.union(
      DSwithUJ,
      DSwithUV
    )

    // 5. 读取时间戳生成watermarker
    val statsDS: DataStream[VisitorStats] = ds.assignTimestampsAndWatermarks(
      WatermarkStrategy.forBoundedOutOfOrderness[VisitorStats](
        Duration.ofSeconds(10)
      )
        .withTimestampAssigner(
          new SerializableTimestampAssigner[VisitorStats] {
            override def extractTimestamp(element: VisitorStats, recordTimestamp: Long) = {
              element.ts
            }
          }
        )
    )

    // 6. 按照维度信息进行分组
    val keyStream: KeyedStream[VisitorStats, (String, String, String, String)] = statsDS.keyBy(
      new KeySelector[VisitorStats, Tuple4[String, String, String, String]]() {
        override def getKey(value: VisitorStats) {
          new Tuple4[String, String, String, String](value.ar,
            value.ch,
            value.is_new,
            value.vc
          )
        }
      }
    )


    // 7.开窗 聚合
    keyStream.window(
      TumblingEventTimeWindows.of(Time.seconds(10))
    ).apply(
      
    )

    


  }
}
